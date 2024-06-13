/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.CachingValueFactory;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.rio.TriGStarParser;
import com.msd.gin.halyard.util.LRUCache;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.rio.ParseErrorListener;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParserFactory;
import org.eclipse.rdf4j.rio.RDFParserRegistry;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParserSettings;
import org.eclipse.rdf4j.rio.trix.TriXParser;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Hadoop MapReduce Tool for bulk loading RDF into HBase
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkLoad extends AbstractHalyardTool {
    private static final Logger LOG = LoggerFactory.getLogger(HalyardBulkLoad.class);

    private static final String TOOL_NAME = "bulkload";

    public static final String TARGET_TABLE_PROPERTY = confProperty(TOOL_NAME, "table.name");

    /**
     * Property defining number of bits used for HBase region pre-splits calculation for new table
     */
    public static final String SPLIT_BITS_PROPERTY = confProperty(TOOL_NAME, "table.splitbits");

    /**
     * Property truncating existing HBase table just before the bulk load
     */
    public static final String TRUNCATE_PROPERTY = confProperty(TOOL_NAME, "table.truncate");

    /**
     * Property defining exact timestamp of all loaded triples (System.currentTimeMillis() is the default value)
     */
    public static final String TIMESTAMP_PROPERTY = confProperty(TOOL_NAME, "timestamp");

    public static final String STATEMENT_DEDUP_CACHE_SIZE_PROPERTY = confProperty(TOOL_NAME, "statement-dedup-cache.size");
    public static final String VALUE_CACHE_SIZE_PROPERTY = confProperty(TOOL_NAME, "value-cache.size");
    public static final String HIDDEN_CONTEXT_PROPERTY = confProperty(TOOL_NAME, "context.hidden");

    /**
     * Boolean property ignoring RDF parsing errors
     */
    public static final String ALLOW_INVALID_IRIS_PROPERTY = confProperty("parser", "allow-invalid-iris");

    /**
     * Boolean property ignoring RDF parsing errors
     */
    public static final String SKIP_INVALID_LINES_PROPERTY = confProperty("parser", "skip-invalid-lines");

    /**
     * Boolean property enabling RDF parser verification of data values
     */
    public static final String VERIFY_DATATYPE_VALUES_PROPERTY = confProperty("parser", "verify-datatype-values");

    /**
     * Boolean property enforcing triples and quads context override with the default context
     */
    public static final String OVERRIDE_CONTEXT_PROPERTY = confProperty("parser", "context.override");

    /**
     * Property defining default context for triples (or even for quads when context override is set)
     */
    public static final String DEFAULT_CONTEXT_PROPERTY = confProperty("parser", "context.default");

    public static final String PARSER_QUEUE_SIZE_PROPERTY = confProperty("parser", "queue.size");

    /**
     * Multiplier limiting maximum single file size in relation to the maximum split size, before it is processed in parallel (10x maximum split size)
     */
    private static final long MAX_SINGLE_FILE_MULTIPLIER = 10;
    private static final int DEFAULT_SPLIT_BITS = 3;
    private static final long DEFAULT_SPLIT_MAXSIZE = 200000000l;
    private static final int DEFAULT_PARSER_QUEUE_SIZE = 50000;
    static final int DEFAULT_STATEMENT_DEDUP_CACHE_SIZE = 2000;
    private static final int DEFAULT_VALUE_CACHE_SIZE = 2000;

    enum Counters {
		ADDED_KVS,
		ADDED_STATEMENTS,
    	TOTAL_STATEMENTS_READ
	}

    static void replaceParser(RDFFormat format, RDFParserFactory newpf) {
        RDFParserRegistry reg = RDFParserRegistry.getInstance();
        reg.get(format).ifPresent(pf -> reg.remove(pf));
        reg.add(newpf);
    }

    static void setParsers() {
        // this is a workaround to avoid autodetection of .xml files as TriX format and hook on .trix file extension only
        replaceParser(RDFFormat.TRIX, new RDFParserFactory() {
            @Override
            public RDFFormat getRDFFormat() {
                RDFFormat t = RDFFormat.TRIX;
                return new RDFFormat(t.getName(), t.getMIMETypes(), t.getCharset(), Arrays.asList("trix"), t.getStandardURI(), t.supportsNamespaces(), t.supportsContexts(), t.supportsRDFStar());
            }

            @Override
            public RDFParser getParser() {
                return new TriXParser();
            }
        });
        // this is a workaround to make Turtle parser more resistant to invalid URIs when in dirty mode
        replaceParser(RDFFormat.TURTLE, new RDFParserFactory() {
            @Override
            public RDFFormat getRDFFormat() {
                return RDFFormat.TURTLE;
            }
            @Override
            public RDFParser getParser() {
                return new TurtleParser(){
                    @Override
                    protected IRI parseURI() throws IOException, RDFParseException {
                        try {
                            return super.parseURI();
                        } catch (RuntimeException e) {
                            if (getParserConfig().get(NTriplesParserSettings.FAIL_ON_INVALID_LINES)) {
                                throw e;
                            } else {
                                reportError(e, NTriplesParserSettings.FAIL_ON_INVALID_LINES);
                                return null;
                            }
                        }
                    }
                    @Override
                    protected Literal createLiteral(String label, String lang, IRI datatype, long lineNo, long columnNo) throws RDFParseException {
                        try {
                            return super.createLiteral(label, lang, datatype, lineNo, columnNo);
                        } catch (RuntimeException e) {
                            if (getParserConfig().get(NTriplesParserSettings.FAIL_ON_INVALID_LINES)) {
                                throw e;
                            } else {
                                reportError(e, NTriplesParserSettings.FAIL_ON_INVALID_LINES);
                                return super.createLiteral(label, (String) null, (IRI) null, lineNo, columnNo);
                            }
                        }
                    }
                };
            }
        });
        // this is a workaround for https://github.com/eclipse/rdf4j/issues/3664
        replaceParser(RDFFormat.TRIGSTAR, new TriGStarParser.Factory());
    }

    /**
     * Mapper class transforming each parsed Statement into set of HBase KeyValues
     */
    public static final class RDFMapper extends Mapper<LongWritable, Statement, ImmutableBytesWritable, KeyValue> implements Function<Statement,List<? extends KeyValue>> {

        private final ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
        private Set<Statement> stmtDedup;
        private StatementIndices stmtIndices;
        private boolean hiddenGraph;
        private long timestamp;
        private long addedKvs;
        private long addedStmts;
        private long totalStmtsRead;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            init(conf);
        }

        void init(Configuration conf) throws IOException {
            stmtDedup = Collections.newSetFromMap(new LRUCache<>(conf.getInt(STATEMENT_DEDUP_CACHE_SIZE_PROPERTY, DEFAULT_STATEMENT_DEDUP_CACHE_SIZE)));
            RDFFactory rdfFactory = RDFFactory.create(conf);
            stmtIndices = new StatementIndices(conf, rdfFactory);
            timestamp = conf.getLong(TIMESTAMP_PROPERTY, System.currentTimeMillis());
            hiddenGraph = conf.getBoolean(HIDDEN_CONTEXT_PROPERTY, false);
        }

        @Override
        protected void map(LongWritable key, Statement stmt, final Context output) throws IOException, InterruptedException {
    		List<? extends KeyValue> kvs = apply(stmt);
        	if (kvs != null) {
	            for (KeyValue keyValue: kvs) {
	                rowKey.set(keyValue.getRowArray(), keyValue.getRowOffset(), keyValue.getRowLength());
	                output.write(rowKey, keyValue);
	                addedKvs++;
	            }
	            addedStmts++;
        	}
        	totalStmtsRead++;
        }

        @Override
        public List<? extends KeyValue> apply(Statement stmt) {
        	// best effort statement deduplication
        	if (!stmtDedup.add(stmt)) {
        		return null;
        	}

        	List<? extends KeyValue> kvs;
    		if (HALYARD.SYSTEM_GRAPH_CONTEXT.equals(stmt.getContext()) || hiddenGraph) {
    			kvs = stmtIndices.insertNonDefaultKeyValues(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), timestamp);
    		} else {
    			kvs = stmtIndices.insertKeyValues(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), timestamp);
    		}
    		return kvs;
        }

        @Override
        protected void cleanup(Context output) throws IOException {
        	output.getCounter(Counters.ADDED_KVS).increment(addedKvs);
        	output.getCounter(Counters.ADDED_STATEMENTS).increment(addedStmts);
        	output.getCounter(Counters.TOTAL_STATEMENTS_READ).increment(totalStmtsRead);
        }
    }

    /**
     * MapReduce FileInputFormat reading and parsing any RDF4J RIO supported RDF format into Statements
     */
    public static final class RioFileInputFormat extends CombineFileInputFormat<LongWritable, Statement> {

        public RioFileInputFormat() {
            setParsers();
        }

        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            List<InputSplit> splits = super.getSplits(job);
            long maxSize = MAX_SINGLE_FILE_MULTIPLIER * job.getConfiguration().getLong(FileInputFormat.SPLIT_MAXSIZE, 0);
            if (maxSize > 0) {
                List<InputSplit> newSplits = new ArrayList<>();
                for (InputSplit spl : splits) {
                    CombineFileSplit cfs = (CombineFileSplit)spl;
                    for (int i=0; i<cfs.getNumPaths(); i++) {
                        long length = cfs.getLength();
                        if (length > maxSize) {
                            int replicas = (int)Math.ceil((double)length / (double)maxSize);
                            Path path = cfs.getPath(i);
                            for (int r=1; r<replicas; r++) {
                                newSplits.add(new CombineFileSplit(new Path[]{path}, new long[]{r}, new long[]{length}, cfs.getLocations()));
                            }
                        }
                    }
                }
                splits.addAll(newSplits);
            }
            return splits;
        }

        @Override
        protected List<FileStatus> listStatus(JobContext job) throws IOException {
            List<FileStatus> filteredList = new ArrayList<>();
            for (FileStatus fs : super.listStatus(job)) {
                if (Rio.getParserFormatForFileName(fs.getPath().getName()).isPresent()) {
                    filteredList.add(fs);
                }
            }
            return filteredList;
        }

        @Override
        public RecordReader<LongWritable, Statement> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
            return new RecordReader<LongWritable, Statement>() {

                private final AtomicLong key = new AtomicLong();

                private ParserPump pump = null;
                private LongWritable currentKey = new LongWritable();
                private Statement currentValue = null;
                private Thread pumpThread = null;

                @Override
                public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                    close();
                    pump = new ParserPump((CombineFileSplit)split, context);
                    pumpThread = new Thread(pump);
                    pumpThread.setDaemon(true);
                    pumpThread.start();
                }

                @Override
                public boolean nextKeyValue() throws IOException, InterruptedException {
                    if (pump != null) {
                        currentValue = pump.getNext();
                        if (currentValue != null) {
                            currentKey.set(key.incrementAndGet());
                            return true;
                        }
                    }
                    currentKey = null;
                    currentValue = null;
                    return false;
                }

                @Override
                public LongWritable getCurrentKey() throws IOException, InterruptedException {
                    return currentKey;
                }

                @Override
                public Statement getCurrentValue() throws IOException, InterruptedException {
                    return currentValue;
                }

                @Override
                public float getProgress() throws IOException, InterruptedException {
                    return pump == null ? 0 : pump.getProgress();
                }

                @Override
                public void close() throws IOException {
                    if (pump != null) {
                        pump.close();
                        pump = null;
                    }
                    if (pumpThread != null) {
                        pumpThread.interrupt();
                        pumpThread = null;
                    }
                }
            };
        }
    }

    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private static final IRI NOP = VF.createIRI(":");
    private static final Statement END_STATEMENT = VF.createStatement(NOP, NOP, NOP);

    private static final class ParserPump extends AbstractRDFHandler implements Closeable, Runnable, ParseErrorListener {

    	enum Counters {
    		PARSE_QUEUE_EMPTY_COUNT,
    		PARSE_QUEUE_EMPTY_ELAPSED_TIME,
    		PARSE_QUEUE_FULL_COUNT,
    		PARSE_QUEUE_FULL_ELAPSED_TIME,
    		PARSE_ERRORS
    	}

        private final BlockingQueue<Statement> queue;
        private final IdValueFactory idValueFactory;
        private final CachingValueFactory valueFactory;
        private final TaskAttemptContext context;
        private final Path paths[];
        private final long[] sizes, offsets;
        private final long size;
        private final boolean allowInvalidIris, skipInvalidLines, verifyDataTypeValues;
        private final String defaultRdfContextPattern;
        private final boolean overrideRdfContext;
        private final long maxSize;
        private volatile String baseUri;
        private volatile Exception ex;
        private long finishedSize = 0L;
        private int offset, count;
        private boolean namespaceContextStatementWritten;

        private InputStream inStream;

        public ParserPump(CombineFileSplit split, TaskAttemptContext context) throws IOException {
            this.context = context;
            this.paths = split.getPaths();
            this.sizes = split.getLengths();
            this.offsets = split.getStartOffsets();
            this.size = split.getLength();
            Configuration conf = context.getConfiguration();
            this.queue = new LinkedBlockingQueue<>(conf.getInt(PARSER_QUEUE_SIZE_PROPERTY, DEFAULT_PARSER_QUEUE_SIZE));
            this.idValueFactory = new IdValueFactory(RDFFactory.create(conf));
            this.valueFactory = new CachingValueFactory(idValueFactory, conf.getInt(VALUE_CACHE_SIZE_PROPERTY, DEFAULT_VALUE_CACHE_SIZE));
            this.allowInvalidIris = conf.getBoolean(ALLOW_INVALID_IRIS_PROPERTY, false);
            this.skipInvalidLines = conf.getBoolean(SKIP_INVALID_LINES_PROPERTY, false);
            this.verifyDataTypeValues = conf.getBoolean(VERIFY_DATATYPE_VALUES_PROPERTY, false);
            this.overrideRdfContext = conf.getBoolean(OVERRIDE_CONTEXT_PROPERTY, false);
            this.defaultRdfContextPattern = conf.get(DEFAULT_CONTEXT_PROPERTY);
            this.maxSize = MAX_SINGLE_FILE_MULTIPLIER * conf.getLong(FileInputFormat.SPLIT_MAXSIZE, 0);
        }

        public Statement getNext() throws IOException, InterruptedException {
            // remove from queue even on error to empty it
            Statement s = queue.poll();
            if (s == null) {
            	context.getCounter(Counters.PARSE_QUEUE_EMPTY_COUNT).increment(1);
        		long startBlocking = System.nanoTime();
            	s = queue.take();
        		long endBlocking = System.nanoTime();
                context.getCounter(Counters.PARSE_QUEUE_EMPTY_ELAPSED_TIME).increment(TimeUnit.NANOSECONDS.toMillis(endBlocking-startBlocking));
            }
            if (ex != null) {
        		throw new IOException("Exception while parsing: " + baseUri, ex);
            }
            return s == END_STATEMENT ? null : s;
        }

        public synchronized float getProgress() {
            try {
                long seekPos = (inStream instanceof Seekable) ? ((Seekable)inStream).getPos() : 0L;
                return (float)(finishedSize + seekPos) / (float)size;
            } catch (IOException e) {
                return (float)finishedSize / (float)size;
            }
        }

        @Override
        public void run() {
            setParsers();
            try {
                Configuration conf = context.getConfiguration();
                for (int i=0; i<paths.length; i++) {
                    synchronized (this) {
                        if (inStream instanceof Seekable) {
                        	try {
	                            finishedSize += ((Seekable)inStream).getPos();  // end position of previous file
	                        } catch (IOException e) {
	                            //ignore
	                        }
                        }
                    }
                    close();
                    Path file = paths[i];
                    final String localBaseUri = file.toString();
                    RDFFormat rdfFormat = Rio.getParserFormatForFileName(localBaseUri).orElse(null);
                    if (rdfFormat != null) {
                        this.baseUri = localBaseUri;
	                    this.offset = (int) offsets[i];
	                    this.count = (maxSize > 0 && sizes[i] > maxSize) ? (int) Math.ceil((double)sizes[i] / (double)maxSize) : 1;
	                    context.setStatus("Parsing " + localBaseUri);
		                try {
		                    FileSystem fs = file.getFileSystem(conf);
		                    FSDataInputStream fileIn = fs.open(file);
		                    CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
		                    final InputStream localStream;
		                    if (codec != null) {
		                    	localStream = codec.createInputStream(fileIn, CodecPool.getDecompressor(codec));
		                    } else {
		                    	localStream = fileIn;
		                    }
		                    synchronized (this) {
		                        this.inStream = localStream; //synchronised parameters must be set inside a sync block
		                    }
		                    RDFParser parser = Rio.createParser(rdfFormat);
		                    parser.setRDFHandler(this);
		                    parser.setParseErrorListener(this);
		                    parser.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
		                    parser.set(BasicParserSettings.VERIFY_URI_SYNTAX, !allowInvalidIris);
		                    parser.set(BasicParserSettings.VERIFY_RELATIVE_URIS, !allowInvalidIris);
		                    if (skipInvalidLines) {
		                        parser.set(NTriplesParserSettings.FAIL_ON_INVALID_LINES, false);
		                        parser.getParserConfig().addNonFatalError(NTriplesParserSettings.FAIL_ON_INVALID_LINES);
		                    }
		                   	parser.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, verifyDataTypeValues);
		                    parser.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, verifyDataTypeValues);
		                    if (defaultRdfContextPattern != null || overrideRdfContext) {
		                        IRI defaultRdfContext;
		                        if (defaultRdfContextPattern != null) {
		                            String context = MessageFormat.format(defaultRdfContextPattern, localBaseUri, file.toUri().getPath(), file.getName());
		                            validateIRIs(context);
		                            defaultRdfContext = valueFactory.createIRI(context);
		                        } else {
		                            defaultRdfContext = null;
		                        }
		                        valueFactory.setDefaultContext(defaultRdfContext, overrideRdfContext);
		                    }
		                    parser.setValueFactory(valueFactory);
		                    parser.parse(localStream, localBaseUri);
		                } catch (Exception e) {
		                    if (allowInvalidIris && skipInvalidLines && !verifyDataTypeValues) {
		                        LOG.warn("Exception while parsing RDF", e);
		                    } else {
		                        throw e;
		                    }
		                }
                    } else {
                    	LOG.warn("Skipping unsupported file: {}", localBaseUri);
                    }
	            }
            } catch (Exception e) {
                ex = e;
            } finally {
                try {
                    queue.put(END_STATEMENT);
                } catch (InterruptedException ignore) {}
            }
        }

        @Override
        public void handleStatement(Statement st) {
            if (count == 1 || Math.floorMod(st.hashCode(), count) == offset) {
            	if (!queue.offer(st)) {
            		context.getCounter(Counters.PARSE_QUEUE_FULL_COUNT).increment(1);
	            	try {
	            		long startBlocking = System.nanoTime();
		                queue.put(st);
		                long endBlocking = System.nanoTime();
		                context.getCounter(Counters.PARSE_QUEUE_FULL_ELAPSED_TIME).increment(TimeUnit.NANOSECONDS.toMillis(endBlocking-startBlocking));
		            } catch (InterruptedException e) {
		                throw new RDFHandlerException(e);
		            }
            	}
            }
        }

        @Override
        public void handleNamespace(String prefix, String uri) {
        	if (!namespaceContextStatementWritten) {
	        	// NB: use idValueFactory so the graph context doesn't get overridden
	    		handleStatement(idValueFactory.createStatement(HALYARD.SYSTEM_GRAPH_CONTEXT, RDF.TYPE, SD.NAMED_GRAPH_CLASS, HALYARD.SYSTEM_GRAPH_CONTEXT));
	    		namespaceContextStatementWritten = true;
        	}
            if (prefix.length() > 0) {
                handleStatement(valueFactory.createStatement(valueFactory.createIRI(uri), HALYARD.NAMESPACE_PREFIX_PROPERTY, valueFactory.createLiteral(prefix), HALYARD.SYSTEM_GRAPH_CONTEXT));
            }
        }

        @Override
        public synchronized void close() throws IOException {
            if (inStream != null) {
                inStream.close();
                inStream = null;
            }
        }

        @Override
        public void warning(String msg, long lineNo, long colNo) {
            LOG.warn(msg);
        }

        @Override
        public void error(String msg, long lineNo, long colNo) {
            LOG.error(msg);
            context.getCounter(Counters.PARSE_ERRORS).increment(1);
        }

        @Override
        public void fatalError(String msg, long lineNo, long colNo) {
            LOG.error(msg);
        }
    }

    static String listRDFFormats() {
        StringBuilder sb = new StringBuilder();
        for (RDFFormat fmt : RDFParserRegistry.getInstance().getKeys()) {
            sb.append("* ").append(fmt.getName()).append(" (");
            boolean first = true;
            for (String ext : fmt.getFileExtensions()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append('.').append(ext);
            }
            sb.append(")\n");
        }
        return sb.toString();
    }

    public HalyardBulkLoad() {
        this(
            TOOL_NAME,
            "Halyard Bulk Load is a MapReduce application designed to efficiently load RDF data from Hadoop Filesystem (HDFS) into HBase in the form of a Halyard dataset.",
            "Halyard Bulk Load consumes RDF files in various formats supported by RDF4J RIO, including:\n"
                + listRDFFormats()
                + "All the supported RDF formats can be also compressed with one of the compression codecs supported by Hadoop, including:\n"
                + "* Gzip (.gz)\n"
                + "* Bzip2 (.bz2)\n"
                + "* LZO (.lzo)\n"
                + "* Snappy (.snappy)\n"
                + "Example: halyard bulkload -s hdfs://my_RDF_files -w hdfs:///my_tmp_workdir -t mydataset [-g 'http://whatever/graph']"
        );
    }

    protected HalyardBulkLoad(String toolName, String header, String footer) {
        super(toolName, header, footer);
        addOption("s", "source", "source_paths", SOURCE_PATHS_PROPERTY, "Source path(s) with RDF files, more paths can be delimited by comma, the paths are recursively searched for the supported files", true, true);
        addOption("w", "work-dir", "shared_folder", "Unique non-existent folder within shared filesystem to server as a working directory for the temporary HBase files,  the files are moved to their final HBase locations during the last stage of the load process", true, true);
        addOption("t", "target", "dataset_table", TARGET_TABLE_PROPERTY, "Target HBase table with Halyard RDF store, target table is created if it does not exist, however optional HBase namespace of the target table must already exist", true, true);
        addOption("i", "allow-invalid-iris", null, ALLOW_INVALID_IRIS_PROPERTY, "Optionally allow invalid IRI values (less overhead)", false, false);
        addOption("d", "verify-data-types", null, VERIFY_DATATYPE_VALUES_PROPERTY, "Optionally verify RDF data type values while parsing", false, false);
        addOption("k", "skip-invalid-lines", null, SKIP_INVALID_LINES_PROPERTY, "Optionally skip invalid lines", false, false);
        addOption("r", "truncate-target", null, TRUNCATE_PROPERTY, "Optionally truncate target table just before the loading the new data", false, false);
        addOption("b", "pre-split-bits", "bits", SPLIT_BITS_PROPERTY, "Optionally specify bit depth of region pre-splits for a case when target table does not exist (default is 3, -1 for no splits)", false, true);
        addOption("g", "default-named-graph", "named_graph", DEFAULT_CONTEXT_PROPERTY, "Optionally specify default target named graph", false, true);
        addOption("o", "named-graph-override", null, OVERRIDE_CONTEXT_PROPERTY, "Optionally override named graph also for quads, named graph is stripped from quads if --default-named-graph option is not specified", false, false);
        addOption("e", "target-timestamp", "timestamp", TIMESTAMP_PROPERTY, "Optionally specify timestamp of all loaded records (default is actual time of the operation)", false, true);
        addOption("m", "max-split-size", "size_in_bytes", FileInputFormat.SPLIT_MAXSIZE, "Optionally override maximum input split size, where significantly larger single files will be processed in parallel (0 means no limit, default is 200000000)", false, true);
        addOption(null, "dry-run", null, DRY_RUN_PROPERTY, "Skip loading of HFiles", false, true);
        addOption(null, "hidden-graph", null, HIDDEN_CONTEXT_PROPERTY, "Load into a hidden named graph (can only be used in conjunction with -g)", false, true);
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
    	configureString(cmd, 's', null);
        String workdir = cmd.getOptionValue('w');
        configureString(cmd, 't', null);
        configureBoolean(cmd, 'i');
        configureBoolean(cmd, 'd');
        configureBoolean(cmd, 'k');
        configureBoolean(cmd, 'r');
        configureInt(cmd, 'b', DEFAULT_SPLIT_BITS);
        configureIRIPattern(cmd, 'g', null);
        configureBoolean(cmd, 'o');
        configureLong(cmd, 'e', System.currentTimeMillis());
        configureLong(cmd, 'm', DEFAULT_SPLIT_MAXSIZE);
        configureBoolean(cmd, "dry-run");
        configureBoolean(cmd, "hidden-graph");
        if (getConf().get(DEFAULT_CONTEXT_PROPERTY) == null && getConf().get(HIDDEN_CONTEXT_PROPERTY) != null) {
        	throw new MissingOptionException("Missing -g with --hidden-graph");
        }
        String sourcePaths = getConf().get(SOURCE_PATHS_PROPERTY);
        String target = getConf().get(TARGET_TABLE_PROPERTY);

        HBaseConfiguration.addHbaseResources(getConf());

        Job job = Job.getInstance(getConf(), "HalyardBulkLoad -> " + workdir + " -> " + target);
        job.setJarByClass(HalyardBulkLoad.class);
        job.setMapperClass(RDFMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setInputFormatClass(RioFileInputFormat.class);
        job.setSpeculativeExecution(false);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, sourcePaths);
        Path outPath = new Path(workdir);
        FileOutputFormat.setOutputPath(job, outPath);

        TableDescriptor tableDesc;
		try (Connection conn = HalyardTableUtils.getConnection(getConf())) {
            try (Table hTable = HalyardTableUtils.getTable(conn, target, true, getConf().getInt(SPLIT_BITS_PROPERTY, DEFAULT_SPLIT_BITS))) {
				tableDesc = hTable.getDescriptor();
				RegionLocator regionLocator = conn.getRegionLocator(tableDesc.getTableName());
				HFileOutputFormat2.configureIncrementalLoad(job, tableDesc, regionLocator);
		        TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
		                NTriplesUtil.class,
		                Rio.class,
		                AbstractRDFHandler.class,
		                RDFFormat.class,
		                RDFParser.class);
	        }
            try (Keyspace keyspace = HalyardTableUtils.getKeyspace(getConf(), conn, tableDesc.getTableName(), null, null)) {
            	try (KeyspaceConnection ksConn = keyspace.getConnection()) {
            		RDFFactory.create(ksConn);  // migrate and validate config early before submitting the job
            		HBaseConfiguration.merge(job.getConfiguration(), HalyardTableUtils.readConfig(ksConn));
            	}
            }
		}
        int rc = run(job, tableDesc);
        if (rc == 0) {
        	TableName tableName = tableDesc.getTableName();
            if (getConf().getBoolean(TRUNCATE_PROPERTY, false)) {
        		try (Connection conn = HalyardTableUtils.getConnection(getConf())) {
        			HalyardTableUtils.clearStatements(conn, tableName);
        		}
            }
    		bulkLoad(job, tableName, outPath);
            LOG.info("Bulk Load completed.");
        } else {
    		LOG.error("Bulk Load failed to complete.");
        }
        return rc;
    }

    protected int run(Job job, TableDescriptor tableDesc) throws Exception {
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
