package com.msd.gin.halyard.tools;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RDFLoadStager implements Callable<Void> {
	private static final Logger LOGGER = LoggerFactory.getLogger(RDFLoadStager.class);

	private static final Map<String,String> COMPRESSION_FORMATS;
	private static final Set<String> MERGEABLE_COMPRESSION_FORMATS = Sets.newHashSet(".gz", ".bz2");
	private static final Set<String> MERGEABLE_RDF_FORMATS = Sets.newHashSet(".nt", ".nq", ".ttl", ".ttls", ".trig", ".trigs");

	static {
		COMPRESSION_FORMATS = new HashMap<>();
		COMPRESSION_FORMATS.put(".gz", CompressorStreamFactory.GZIP);
		COMPRESSION_FORMATS.put(".bz2", CompressorStreamFactory.BZIP2);
	}

	private static final long MB = 1024*1024;
	private static final long GB = 1024*MB;
	private static final long MIN_BLOCK_SIZE = 64*MB;
	private static final int MAX_SMALL_PARTITIONS = 4;

	public static void main(String[] args) throws Exception {
		Path rdfData = Paths.get(args[0]);
		org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(args[1]);
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration conf = new Configuration();
		int maxPartitions = Integer.MAX_VALUE;
		String compressionExt = null;
		int i=2;
		while (i+1<args.length) {
			String argName = args[i++];
			String argValue = args[i++];
			if (argName.equals("-conf")) {
				org.apache.hadoop.fs.Path confFile = new org.apache.hadoop.fs.Path(argValue);
				conf.addResource(confFile);
			} else if (argName.equals("--max-partitions")) {
				maxPartitions = Integer.parseInt(argValue);
			} else if (argName.equals("--compress")) {
				compressionExt = argValue;
			} else {
				throw new Exception("Invalid command line arguments: "+argName+" "+argValue);
			}
		}
		if (i < args.length) {
			throw new Exception("Invalid command line arguments: "+String.join(" ", Arrays.asList(args).subList(i, args.length)));
		}
		new RDFLoadStager(conf, rdfData, hdfsPath, maxPartitions, compressionExt).call();
	}

	private final Configuration conf;
	private final Path rdfData;
	private final org.apache.hadoop.fs.Path hdfsPath;
	private final int bufferSize;
	private final short replication;
	private final long defaultBlockSize;
	private final int maxPartitions;
	private final String compressionExt;

	RDFLoadStager(Configuration conf, Path rdfData, org.apache.hadoop.fs.Path hdfsPath, int maxPartitions, String compressionExt) {
		this.conf = conf;
		this.rdfData = rdfData;
		this.hdfsPath = hdfsPath;
		this.maxPartitions = maxPartitions;
		this.compressionExt = checkIsCompressionExt(compressionExt);
		this.bufferSize = Integer.getInteger(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT));
		this.replication = Integer.getInteger(HdfsClientConfigKeys.DFS_REPLICATION_KEY, conf.getInt(HdfsClientConfigKeys.DFS_REPLICATION_KEY, HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT)).shortValue();
		this.defaultBlockSize = Long.getLong(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, conf.getLong(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT));
	}

	public Void call() throws Exception {
		// get all files
		Map<FileExtension, List<FileInfo>> filesByExt = new HashMap<>();
		Files.walkFileTree(rdfData, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				FileVisitResult r = super.visitFile(file, attrs);
				FileInfo fi = new FileInfo(file);
				FileExtension mergeExt = (compressionExt != null) ? new FileExtension(fi.extension.type, null) : fi.extension;
				filesByExt.compute(mergeExt, (k,v) -> {
					if (v == null) {
						v = new ArrayList<>();
					}
					v.add(fi);
					return v;
				});
				return r;
			}
		});

		ExecutorService executor = Executors.newCachedThreadPool();
		ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
		int numTasks = 0;
		for (Map.Entry<FileExtension, List<FileInfo>> entry : filesByExt.entrySet()) {
			FileExtension mergeExt = entry.getKey();
			FileExtension outputExt = (compressionExt != null) ? new FileExtension(mergeExt.type, compressionExt) : mergeExt;
			List<FileInfo> files = entry.getValue();
			if (files.size() > 1 && canMerge(mergeExt)) {
				int numPartitions;
				long totalSize = sizeOf(files);
				if (totalSize <= MIN_BLOCK_SIZE) {
					numPartitions = 1;
				} else if (totalSize <= GB) {
					numPartitions = Math.min((int) (totalSize/MIN_BLOCK_SIZE), MAX_SMALL_PARTITIONS);
				} else {
					numPartitions = (int) (totalSize/defaultBlockSize) + 1;
				}
				LOGGER.info("Merging {} bytes of data to {}...", totalSize, outputExt);

				// don't create more partitions than files
				numPartitions = Math.min(numPartitions, files.size());
				numPartitions = Math.min(numPartitions, maxPartitions);
				List<Partition> partitions = new ArrayList<>(numPartitions);
				if (numPartitions > 1) {
					for (int i=0; i<numPartitions; i++) {
						partitions.add(new Partition(hdfsPath, i, outputExt));
					}
					// "Longest-processing-time-first scheduling"
					// sort large to small
					PriorityQueue<Partition> totalSizeQueue = new PriorityQueue<>(new Comparator<Partition>() {
						@Override
						public int compare(Partition o1, Partition o2) {
							return Long.compare(o1.size, o2.size);
						}
					});
					for (Partition partition : partitions) {
						totalSizeQueue.add(partition);
					}
					files.sort(new LargeToSmallFileSizeComparator());
					for (FileInfo fi : files) {
						Partition p = totalSizeQueue.remove();
						p.add(fi);
						totalSizeQueue.add(p);
					}
				} else {
					Partition partition = new Partition(hdfsPath, 0, outputExt);
					files.stream().forEach(fi -> partition.add(fi));
					partitions.add(partition);
				}
				for (Partition p : partitions) {
					LOGGER.info(p.toString());
					completionService.submit(new FileMergeTask(p));
					numTasks++;
				}
			} else {
				for (FileInfo fi : files) {
					completionService.submit(new FileCopyTask(fi, outputExt));
					numTasks++;
				}
			}
		}

		for (int i=0; i<numTasks; i++) {
			try {
				completionService.take().get();
			} catch (InterruptedException e) {
				executor.shutdownNow();
				throw e;
			} catch (ExecutionException e) {
				executor.shutdownNow();
				Throwable thr = e.getCause();
				if (thr instanceof Exception) {
					throw (Exception) thr;
				} else {
					throw e;
				}
			}
		}

		return null;
	}

	private static boolean canMerge(FileExtension ext) {
		return MERGEABLE_RDF_FORMATS.contains(ext.type) && (ext.compression == null || MERGEABLE_COMPRESSION_FORMATS.contains(ext.compression));
	}

	private long blockSize(long fileSize) {
		if (fileSize < defaultBlockSize/4) {
			return Math.max(defaultBlockSize/4, MIN_BLOCK_SIZE);
		} else if (fileSize < defaultBlockSize/2) {
			return Math.max(defaultBlockSize/2, MIN_BLOCK_SIZE);
		} else {
			return defaultBlockSize;
		}
	}

	private static String getCompressionFormat(String ext) {
		if (ext != null) {
			String format = COMPRESSION_FORMATS.get(ext);
			if (format == null) {
				throw new IllegalArgumentException(String.format("Unsupported file compression: %s", ext));
			}
			return format;
		} else {
			return null;
		}
	}

	private static String checkIsCompressionExt(String ext) {
		getCompressionFormat(ext);
		return ext;
	}

	private static int findExtDot(String filename) {
		// NB: start at 1 to handle dot-files
		return filename.indexOf('.', 1);
	}

	private OutputStream getOutput(OutputStream out) throws IOException, CompressorException {
		return RDFSplitter.compress(getCompressionFormat(compressionExt), out);
	}

	private InputStream getInput(FileInfo fi) throws IOException {
		InputStream in = Files.newInputStream(fi.file);
		if (compressionExt != null) {
			// need to decompress file
			in = RDFSplitter.decompress(getCompressionFormat(fi.extension.compression), in);
		}
		return in;
	}

	private static long sizeOf(List<FileInfo> files) {
		return files.stream().mapToLong(fi -> fi.size).sum();
	}

	final class FileMergeTask implements Callable<Void> {
		final Partition p;

		FileMergeTask(Partition p) {
			this.p = p;
		}

		@Override
		public Void call() throws Exception {
			long blockSize = blockSize(p.size);
			FileSystem hdfs = hdfsPath.getFileSystem(conf);
			try (OutputStream out = getOutput(hdfs.create(p.file, true, bufferSize, replication, blockSize))) {
				for (FileInfo fi : p.parts) {
					try (InputStream in = getInput(fi)) {
						IOUtils.copy(in, out, bufferSize);
					} catch (Exception e) {
						LOGGER.error("Failed to transfer {}", fi, e);
						throw e;
					}
				}
			}
			return null;
		}
	}

	final class FileCopyTask implements Callable<Void> {
		final FileInfo fileInfo;
		final org.apache.hadoop.fs.Path hdfsFile;

		FileCopyTask(FileInfo fileInfo, FileExtension outputExt) {
			this.fileInfo = fileInfo;
			Path relFile = rdfData.relativize(fileInfo.file);
			String filename = relFile.getFileName().toString();
			String parent = (relFile.getParent() != null) ? relFile.getParent().toString()+"/" : "";
			int dotPos = findExtDot(filename);
			String base = (dotPos != -1) ? filename.substring(0, dotPos) : filename;
			this.hdfsFile = new org.apache.hadoop.fs.Path(hdfsPath, parent+base+outputExt);
		}

		@Override
		public Void call() throws Exception {
			long blockSize = blockSize(fileInfo.size);
			FileSystem hdfs = hdfsPath.getFileSystem(conf);
			try (OutputStream out = getOutput(hdfs.create(hdfsFile, true, bufferSize, replication, blockSize))) {
				try (InputStream in = getInput(fileInfo)) {
					IOUtils.copy(in, out, bufferSize);
				} catch (Exception e) {
					LOGGER.error("Failed to transfer {}", fileInfo, e);
					throw e;
				}
			}
			return null;
		}
	}


	static final class FileInfo {
		final Path file;
		final long size;
		final FileExtension extension;

		FileInfo(Path f) throws IOException {
			file = f;
			size = Files.size(f);
			extension = FileExtension.getExtension(f.getFileName().toString());
		}

		@Override
		public boolean equals(Object o) {
			return (this == o) || ((o instanceof FileInfo) && file.equals(((FileInfo)o).file));
		}

		@Override
		public int hashCode() {
			return file.hashCode();
		}

		@Override
		public String toString() {
			return file.toString();
		}
	}

	static final class Partition {
		final org.apache.hadoop.fs.Path file;
		final List<FileInfo> parts = new ArrayList<>();
		long size = 0L;

		Partition(org.apache.hadoop.fs.Path outputDir, int n, FileExtension ext) {
			file = new org.apache.hadoop.fs.Path(outputDir, "partition"+(n+1)+ext);
		}

		void add(FileInfo fi) {
			parts.add(fi);
			size += fi.size;
		}

		@Override
		public String toString() {
			return String.format("Partition %s: size = %d, files = %s", file, size, parts);
		}
	}

	static final class LargeToSmallFileSizeComparator implements Comparator<FileInfo> {
		@Override
		public int compare(FileInfo o1, FileInfo o2) {
			return Long.compare(o2.size, o1.size);
		}
	}

	static final class FileExtension {
		final String type;
		final String compression;

		static FileExtension getExtension(String filename) {
			int extPos = findExtDot(filename);
			if (extPos != -1) {
				int dotPos = filename.lastIndexOf('.');
				if (dotPos > extPos) {
					String part1 = filename.substring(extPos, dotPos);
					String part2 = filename.substring(dotPos);
					if (COMPRESSION_FORMATS.containsKey(part2)) {
						return new FileExtension(part1, part2);
					} else {
						return new FileExtension(part2, null);
					}
				} else {
					String ext = filename.substring(extPos);
					if (COMPRESSION_FORMATS.containsKey(ext)) {
						return new FileExtension(null, ext);
					} else {
						return new FileExtension(ext, null);
					}
				}
			} else {
				return new FileExtension(null, null);
			}
		}

		FileExtension(String type, String compression) {
			if (compression != null && !COMPRESSION_FORMATS.containsKey(compression)) {
				throw new IllegalArgumentException(String.format("Unsupported file compression: %s", compression));
			}
			this.type = type;
			this.compression = compression;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o instanceof FileExtension) {
				FileExtension other = (FileExtension) o;
				return Objects.equals(this.type, other.type) && Objects.equals(this.compression, other.compression);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.type, this.compression);
		}

		@Override
		public String toString() {
			return Strings.nullToEmpty(this.type) + Strings.nullToEmpty(this.compression);
		}
	}
}
