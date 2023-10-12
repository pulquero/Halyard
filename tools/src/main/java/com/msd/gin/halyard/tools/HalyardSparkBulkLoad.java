package com.msd.gin.halyard.tools;

import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.spark.FamilyHFileWriteOptions;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.eclipse.rdf4j.model.Statement;

public class HalyardSparkBulkLoad extends HalyardBulkLoad {
	private static final String TOOL_NAME = "bulkload";

	HalyardSparkBulkLoad() {
        super(
            TOOL_NAME,
            "Halyard Spark Bulk Load is a MapReduce application designed to efficiently load RDF data from Hadoop Filesystem (HDFS) into HBase in the form of a Halyard dataset.",
            "Halyard Spark Bulk Load consumes RDF files in various formats supported by RDF4J RIO, including:\n"
                + listRDFFormats()
                + "All the supported RDF formats can be also compressed with one of the compression codecs supported by Hadoop, including:\n"
                + "* Gzip (.gz)\n"
                + "* Bzip2 (.bz2)\n"
                + "* LZO (.lzo)\n"
                + "* Snappy (.snappy)\n"
                + "Example: halyardspark bulkload -s hdfs://my_RDF_files -w hdfs:///my_tmp_workdir -t mydataset [-g 'http://whatever/graph']"
        );
	}

	@Override
	protected int run(Job job, TableDescriptor tableDesc) throws Exception {
		Configuration conf = job.getConfiguration();
		addBloomFilterConfig(conf, tableDesc.getTableName());
        Path workDir = FileOutputFormat.getOutputPath(job);
		SparkConf sparkConf = new SparkConf().setAppName(TOOL_NAME + " " + tableDesc.getTableName().getNameAsString());
		try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
			JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, conf);
			Broadcast<SerializableWritable<Configuration>> broadcastConf = sc.broadcast(new SerializableWritable<>(conf));
			JavaPairRDD<LongWritable,Statement> rdd = sc.newAPIHadoopRDD(conf, RioFileInputFormat.class, LongWritable.class, Statement.class);
			JavaRDD<? extends KeyValue> kvRdd = rdd.values().mapPartitions(partition -> toKeyValues(partition, broadcastConf)).flatMap(List::iterator);

			Map<byte[], FamilyHFileWriteOptions> colFamilyOpts = toWriteOptions(tableDesc);
			long maxFileSize = conf.getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE);
			hbaseContext.bulkLoad(kvRdd, tableDesc.getTableName(), HalyardSparkBulkLoad::toCell,
				workDir.toString(), colFamilyOpts, false, maxFileSize);
		}
		return 0;
	}

	private static Iterator<List<? extends KeyValue>> toKeyValues(Iterator<Statement> partition, Broadcast<SerializableWritable<Configuration>> broadcastConf) throws IOException {
		Configuration conf = broadcastConf.value().value();
		RDFMapper mapper = new RDFMapper();
		mapper.init(conf);
		return Iterators.filter(Iterators.transform(partition, mapper::apply), Objects::nonNull);
	}

	private static Pair<KeyFamilyQualifier,byte[]> toCell(KeyValue kv) {
		KeyFamilyQualifier kfq = new KeyFamilyQualifier(
			Bytes.copy(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()),
			Bytes.copy(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength()),
			Bytes.copy(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength())
		);
		byte[] value = Bytes.copy(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
		return Pair.newPair(kfq, value);
	}

	private static Map<byte[], FamilyHFileWriteOptions> toWriteOptions(TableDescriptor tableDesc) {
		Map<byte[], FamilyHFileWriteOptions> colFamilyOpts = new HashMap<>(2*tableDesc.getColumnFamilyCount());
		for (ColumnFamilyDescriptor colFamilyDesc : tableDesc.getColumnFamilies()) {
			colFamilyOpts.put(colFamilyDesc.getName(), new FamilyHFileWriteOptions(colFamilyDesc.getCompressionType().toString(), colFamilyDesc.getBloomFilterType().toString(), colFamilyDesc.getBlocksize(), colFamilyDesc.getDataBlockEncoding().toString()));
		}
		return colFamilyOpts;
	}
}
