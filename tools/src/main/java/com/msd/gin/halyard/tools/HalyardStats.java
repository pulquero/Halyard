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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.htrace.Trace;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.eclipse.rdf4j.sail.SailConnection;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;
import com.yammer.metrics.core.Gauge;

/**
 * MapReduce tool providing statistics about a Halyard dataset. Statistics about a dataset are reported in RDF using the VOID ontology. These statistics can be useful
 * to summarize a graph and it implicitly shows how the subjects, predicates and objects are used. In the absence of schema information this information can be vital.
 * @author Adam Sotona (MSD)
 */
public final class HalyardStats extends AbstractHalyardTool {

    private static final String SOURCE = "halyard.stats.source";
    private static final String TARGET = "halyard.stats.target";
    private static final String THRESHOLD = "halyard.stats.threshold";
    private static final String TARGET_GRAPH = "halyard.stats.target.graph";
    private static final String GRAPH_CONTEXT = "halyard.stats.graph.context";

    static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    static final class StatsMapper extends TableMapper<ImmutableBytesWritable, LongWritable>  {

        Value lastValue;
        Resource lastClass;
        IRI statsContext, graphContext;
        byte lastRegion = -1;
        long counter = 0;
        boolean update;

        IRI graph = HALYARD.STATS_ROOT_NODE, lastGraph;
        long triples, distinctSubjects, properties, distinctObjects, classes, removed;
        long distinctIRIReferenceSubjects, distinctIRIReferenceObjects, distinctBlankNodeObjects, distinctBlankNodeSubjects, distinctLiterals;
        IRI subsetType;
		Value subsetId;
        long threshold, setCounter, subsetCounter;
        HBaseSail sail;
		SailConnection conn;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            update = conf.get(TARGET) == null;
            threshold = conf.getLong(THRESHOLD, 125);
            statsContext = SVF.createIRI(conf.get(TARGET_GRAPH, HALYARD.STATS_GRAPH_CONTEXT.stringValue()));
            String gc = conf.get(GRAPH_CONTEXT);
            if (gc != null) graphContext = SVF.createIRI(gc);
        }

        private boolean matchingGraphContext(Resource subject) {
            return graphContext == null
                || subject.equals(graphContext)
                || subject.stringValue().startsWith(graphContext.stringValue() + "_subject_")
                || subject.stringValue().startsWith(graphContext.stringValue() + "_property_")
                || subject.stringValue().startsWith(graphContext.stringValue() + "_object_");
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context output) throws IOException, InterruptedException {
        	byte saltedRegion = key.get()[key.getOffset()];
            byte region = HalyardTableUtils.unsalt(saltedRegion);

            for(Statement st : HalyardTableUtils.parseStatements(null, null, null, null, value, SVF)) {
                if (region < HalyardTableUtils.CSPO_PREFIX) {
                	// triple region
                	graph = HALYARD.STATS_ROOT_NODE;
                } else {
                	// quad region
					graph = (IRI) st.getContext();
                }

                if (region != lastRegion || !graph.equals(lastGraph)) {
                    cleanup(output);
                	lastValue = null;
                	lastClass = null;
                }

                if (update && region == HalyardTableUtils.CSPO_PREFIX) {
                    if (statsContext.equals(graph)) {
                        if (sail == null) {
                            Configuration conf = output.getConfiguration();
                            sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null, null);
                            sail.initialize();
							conn = sail.getConnection();
                        }
                        if (matchingGraphContext(st.getSubject())) {
							conn.removeStatement(null, st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
                            removed++;
                        }
                        lastRegion = region;
                        lastGraph = graph;
                        return; //do no count removed statements
                    }
                }

                boolean valueChanged;
                switch (region) {
                    case HalyardTableUtils.SPO_PREFIX:
                    case HalyardTableUtils.CSPO_PREFIX:
                    	valueChanged = !st.getSubject().equals(lastValue);
                    	lastValue = st.getSubject();
                        break;
                    case HalyardTableUtils.POS_PREFIX:
                    case HalyardTableUtils.CPOS_PREFIX:
                    	valueChanged = !st.getPredicate().equals(lastValue);
                    	lastValue = st.getPredicate();
                        break;
                    case HalyardTableUtils.OSP_PREFIX:
                    case HalyardTableUtils.COSP_PREFIX:
                    	valueChanged = !st.getObject().equals(lastValue);
                    	lastValue = st.getObject();
                        break;
                    default:
                        throw new IOException("Unknown region #" + region);
                }

                if (valueChanged) {
               		cleanupSubset(output);
                    switch (region) {
                        case HalyardTableUtils.SPO_PREFIX:
                        case HalyardTableUtils.CSPO_PREFIX:
                            distinctSubjects++;
                            Resource subj = st.getSubject();
                            if (subj instanceof IRI) {
                                distinctIRIReferenceSubjects++;
                            } else {
                                distinctBlankNodeSubjects++;
                            }
                            subsetType = VOID_EXT.SUBJECT;
                            subsetId = subj;
                            break;
                        case HalyardTableUtils.POS_PREFIX:
                        case HalyardTableUtils.CPOS_PREFIX:
                            properties++;
                            subsetType = VOID.PROPERTY;
                            subsetId = st.getPredicate();
                            break;
                        case HalyardTableUtils.OSP_PREFIX:
                        case HalyardTableUtils.COSP_PREFIX:
                            distinctObjects++;
                            Value obj = st.getObject();
                            if (obj instanceof IRI) {
                            	distinctIRIReferenceObjects++;
                            } else if (obj instanceof BNode) {
                            	distinctBlankNodeObjects++;
                            } else {
                                distinctLiterals++;
                            }
                            subsetType = VOID_EXT.OBJECT;
                            subsetId = obj;
                            break;
                        default:
                            throw new IOException("Unknown region #" + region);
                    }
                }

                switch (region) {
                    case HalyardTableUtils.SPO_PREFIX:
                    case HalyardTableUtils.CSPO_PREFIX:
                        triples++;
                        break;
                    case HalyardTableUtils.POS_PREFIX:
                    case HalyardTableUtils.CPOS_PREFIX:
                    	if(st.getObject() instanceof Resource) {
                        	boolean classChanged = !st.getObject().equals(lastClass);
	                    	lastClass = (Resource) st.getObject();
	                        if (RDF.TYPE.equals(lastValue) && classChanged) {
	                        	classes++;
	                        }
                    	}
                        break;
                    default:
                }
                subsetCounter++;
                setCounter++;
                lastRegion = region;
                lastGraph = graph;
                if ((counter++ % 100000) == 0) {
                    output.setStatus(MessageFormat.format("reg:{0} {1} t:{2} s:{3} p:{4} o:{5} c:{6} r:{7}", region, counter, triples, distinctSubjects, properties, distinctObjects, classes, removed));
                }

            }
        }

		private void report(Context output, IRI property, Value partitionId, long value) throws IOException, InterruptedException {
            if (value > 0 && (graphContext == null || graphContext.equals(lastGraph))) {
            	ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
            	try(DataOutputStream dos = new DataOutputStream(baos)) {
            		byte[] graphSer = HalyardTableUtils.writeBytes(lastGraph);
            		dos.writeShort(graphSer.length);
	                dos.write(graphSer);

	                byte[] propertySer = HalyardTableUtils.writeBytes(property);
            		dos.writeShort(propertySer.length);
	                dos.write(propertySer);

	                if(partitionId != null) {
	                	byte[] partitionIdSer = HalyardTableUtils.writeBytes(partitionId);
	                	dos.writeShort(partitionIdSer.length);
	                	dos.write(partitionIdSer);
	                } else {
	                	dos.writeShort(0);
	                }
                }
                output.write(new ImmutableBytesWritable(baos.toByteArray()), new LongWritable(value));
            }
        }

        protected void cleanupSubset(Context output) throws IOException, InterruptedException {
            if (lastGraph != null && subsetCounter >= threshold) {
                report(output, subsetType, subsetId, subsetCounter);
            }
            subsetCounter = 0;
        }

        @Override
        protected void cleanup(Context output) throws IOException, InterruptedException {
        	if (lastGraph != null) {
	        	if (lastGraph == HALYARD.STATS_ROOT_NODE || setCounter >= threshold) {
	                report(output, VOID.TRIPLES, null, triples);
	                report(output, VOID.DISTINCT_SUBJECTS, null, distinctSubjects);
	                report(output, VOID.PROPERTIES, null, properties);
	                report(output, VOID.DISTINCT_OBJECTS, null, distinctObjects);
	                report(output, VOID.CLASSES, null, classes);
	                report(output, VOID_EXT.DISTINCT_IRI_REFERENCE_OBJECTS, null, distinctIRIReferenceObjects);
	                report(output, VOID_EXT.DISTINCT_IRI_REFERENCE_SUBJECTS, null, distinctIRIReferenceSubjects);
	                report(output, VOID_EXT.DISTINCT_BLANK_NODE_OBJECTS, null, distinctBlankNodeObjects);
	                report(output, VOID_EXT.DISTINCT_BLANK_NODE_SUBJECTS, null, distinctBlankNodeSubjects);
	                report(output, VOID_EXT.DISTINCT_LITERALS, null, distinctLiterals);
	            } else {
	                report(output, SD.NAMED_GRAPH_PROPERTY, null, 1);
	            }
        	}
            setCounter = 0;
            triples = 0;
            distinctSubjects = 0;
            properties = 0;
            distinctObjects = 0;
            classes = 0;
            distinctIRIReferenceObjects = 0;
            distinctIRIReferenceSubjects = 0;
            distinctBlankNodeObjects = 0;
            distinctBlankNodeSubjects = 0;
            distinctLiterals = 0;
            cleanupSubset(output);
            if (sail != null) {
				conn.close();
				conn = null;
				sail.shutDown();
                sail = null;
            }
        }

    }

    static final class StatsPartitioner extends Partitioner<ImmutableBytesWritable, LongWritable> {
        @Override
        public int getPartition(ImmutableBytesWritable key, LongWritable value, int numPartitions) {
        	ByteBuffer bb = ByteBuffer.wrap(key.get(), key.getOffset(), key.getLength());
        	bb.limit(2);
        	int size = bb.getShort();
        	bb.limit(bb.limit()+size);
        	Value v = HalyardTableUtils.readValue(bb, SVF);
            return (v.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    static final class StatsReducer extends Reducer<ImmutableBytesWritable, LongWritable, NullWritable, NullWritable>  {

        final Cache<IRI,Boolean> graphs = CacheBuilder.newBuilder().maximumSize(1000).build();
        OutputStream out;
        RDFWriter writer;
        IRI statsGraphContext;
        HBaseSail sail;
		SailConnection conn;
        long removed = 0, added = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            statsGraphContext = SVF.createIRI(conf.get(TARGET_GRAPH, HALYARD.STATS_GRAPH_CONTEXT.stringValue()));
            String targetUrl = conf.get(TARGET);
            if (targetUrl == null) {
                sail = new HBaseSail(conf, conf.get(SOURCE), false, 0, true, 0, null, null);
                sail.initialize();
				conn = sail.getConnection();
				conn.setNamespace(SD.PREFIX, SD.NAMESPACE);
				conn.setNamespace(VOID.PREFIX, VOID.NAMESPACE);
				conn.setNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
				conn.setNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
            } else {
                targetUrl = MessageFormat.format(targetUrl, context.getTaskAttemptID().getTaskID().getId());
                out = FileSystem.get(URI.create(targetUrl), conf).create(new Path(targetUrl));
                try {
                    if (targetUrl.endsWith(".bz2")) {
                        out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, out);
                        targetUrl = targetUrl.substring(0, targetUrl.length() - 4);
                    } else if (targetUrl.endsWith(".gz")) {
                        out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, out);
                        targetUrl = targetUrl.substring(0, targetUrl.length() - 3);
                    }
                } catch (CompressorException ce) {
                    throw new IOException(ce);
                }
                Optional<RDFFormat> form = Rio.getWriterFormatForFileName(targetUrl);
                if (!form.isPresent()) throw new IOException("Unsupported target file format extension: " + targetUrl);
                writer = Rio.createWriter(form.get(), out);
                writer.handleNamespace(SD.PREFIX, SD.NAMESPACE);
                writer.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
                writer.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
                writer.handleNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
                writer.startRDF();
            }
            if (conf.get(GRAPH_CONTEXT) == null) {
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, VOID.DATASET);
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.DATASET);
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.GRAPH_CLASS);
                writeStatement(HALYARD.STATS_ROOT_NODE, SD.DEFAULT_GRAPH, HALYARD.STATS_ROOT_NODE);
            }
        }

        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values) {
                    count += val.get();
            }

        	ByteBuffer bb = ByteBuffer.wrap(key.get(), key.getOffset(), key.getLength());
        	bb.limit(2);
        	int size = bb.getShort();
        	bb.limit(bb.limit()+size);
        	IRI graph = (IRI) HalyardTableUtils.readValue(bb, SVF);
        	bb.limit(bb.limit()+2);
        	size = bb.getShort();
        	bb.limit(bb.limit()+size);
        	IRI predicate = (IRI) HalyardTableUtils.readValue(bb, SVF);
        	bb.limit(bb.limit()+2);
        	size = bb.getShort();
        	Value partitionId;
        	if (size > 0) {
        		bb.limit(bb.limit()+size);
        		partitionId = HalyardTableUtils.readValue(bb, SVF);
        	} else {
        		partitionId = null;
        	}

            
            if (SD.NAMED_GRAPH_PROPERTY.equals(predicate)) { //workaround to at least count all small named graph that are below the threshold
                writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graph);
            } else {
                if (!HALYARD.STATS_ROOT_NODE.equals(graph)) {
                    if (graphs.getIfPresent(graph) == null) { // avoid writing multiple times
                        writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graph);
                        writeStatement(graph, SD.NAME, graph);
                        writeStatement(graph, SD.GRAPH_PROPERTY, graph);
                        writeStatement(graph, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
                        writeStatement(graph, RDF.TYPE, SD.GRAPH_CLASS);
                        writeStatement(graph, RDF.TYPE, VOID.DATASET);
                        graphs.put(graph, Boolean.TRUE);
                    }
                }
                if (partitionId != null) {
					IRI subset = SVF.createIRI(graph + "_" + predicate.getLocalName() + "_" + HalyardTableUtils.encode(HalyardTableUtils.id(partitionId)));
                    writeStatement(graph, SVF.createIRI(predicate + "Partition"), subset);
                    writeStatement(subset, RDF.TYPE, VOID.DATASET);
					writeStatement(subset, predicate, partitionId);
                    writeStatement(subset, VOID.TRIPLES, SVF.createLiteral(count));
                } else {
                    writeStatement(graph, predicate, SVF.createLiteral(count));
                }
                if ((added % 1000) == 0) {
                    context.setStatus(MessageFormat.format("statements removed: {0} added: {1}", removed, added));
                }
            }
        }

        private void writeStatement(Resource subj, IRI pred, Value obj) {
            if (writer == null) {
				conn.addStatement(subj, pred, obj, statsGraphContext);
            } else {
                writer.handleStatement(SVF.createStatement(subj, pred, obj, statsGraphContext));
            }
            added++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (writer == null) {
				conn.close();
				sail.shutDown();
            } else {
                writer.endRDF();
                out.close();
            }
        }
    }

    public HalyardStats() {
        super(
            "stats",
            "Halyard Stats is a MapReduce application that calculates dataset statistics and stores them in the named graph within the dataset or exports them into a file. The generated statistics are described by the VoID vocabulary, its extensions, and the SPARQL 1.1 Service Description.",
            "Example: halyard stats -s my_dataset [-g 'http://whatever/mystats'] [-t hdfs:/my_folder/my_stats.trig]");
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-file", "target_url", "Optional target file to export the statistics (instead of update) hdfs://<path>/<file_name>[{0}].<RDF_ext>[.<compression>]", false, true);
        addOption("r", "threshold", "size", "Optional minimal size of a named graph to calculate statistics for (default is 1000)", false, true);
        addOption("c", "named-graph", "named_graph", "Optional restrict stats calculation to the given named graph only", false, true);
        addOption("g", "stats-named-graph", "target_graph", "Optional target named graph of the exported statistics (default value is '" + HALYARD.STATS_GRAPH_CONTEXT.stringValue() + "'), modification is recomended only for external export as internal Halyard optimizers expect the default value", false, true);
    }

    private static void appendScans(List<Scan> scans, TableName tableName, byte prefix, byte[] key1, byte[] stopKey2, byte[] stopKey3, byte[] stopKey4) throws IOException {
        Scan scan = HalyardTableUtils.scan(HalyardTableUtils.concat(prefix, false, key1), HalyardTableUtils.concat(prefix, true, key1, stopKey2, stopKey3, stopKey4));
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.toBytes());
        HalyardTableUtils.appendSaltedScans(scan, scans);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String target = cmd.getOptionValue('t');
        String targetGraph = cmd.getOptionValue('g');
        String graphContext = cmd.getOptionValue('c');
        String thresh = cmd.getOptionValue('r');
        TableMapReduceUtil.addDependencyJars(getConf(),
               HalyardExport.class,
               NTriplesUtil.class,
               Rio.class,
               AbstractRDFHandler.class,
               RDFFormat.class,
               RDFParser.class,
               HTable.class,
               HBaseConfiguration.class,
               AuthenticationProtos.class,
               Trace.class,
               Gauge.class);
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardStats " + source + (target == null ? " update" : " -> " + target));
        job.getConfiguration().set(SOURCE, source);
        if (target != null) job.getConfiguration().set(TARGET, target);
        if (targetGraph != null) job.getConfiguration().set(TARGET_GRAPH, targetGraph);
        if (graphContext != null) job.getConfiguration().set(GRAPH_CONTEXT, graphContext);
        if (thresh != null) job.getConfiguration().setLong(THRESHOLD, Long.parseLong(thresh));
        job.setJarByClass(HalyardStats.class);
        TableMapReduceUtil.initCredentials(job);

        TableName sourceTableName = TableName.valueOf(source);
        List<Scan> scans = new ArrayList<>();
        if (graphContext != null) { //restricting stats to scan given graph context only
			byte[] gcHash = RDFContext.create(SVF.createIRI(graphContext)).getKeyHash();
            appendScans(scans, sourceTableName, HalyardTableUtils.CSPO_PREFIX, gcHash, RDFSubject.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY);
            appendScans(scans, sourceTableName, HalyardTableUtils.CPOS_PREFIX, gcHash, RDFPredicate.STOP_KEY, RDFObject.STOP_KEY, RDFSubject.END_STOP_KEY);
            appendScans(scans, sourceTableName, HalyardTableUtils.COSP_PREFIX, gcHash, RDFObject.STOP_KEY, RDFSubject.STOP_KEY, RDFPredicate.END_STOP_KEY);
            if (target == null) { //add stats context to the scanned row ranges (when in update mode) to delete the related stats during MapReduce
            	appendScans(scans, sourceTableName, HalyardTableUtils.CSPO_PREFIX,
						RDFContext.create(targetGraph == null ? HALYARD.STATS_GRAPH_CONTEXT : SVF.createIRI(targetGraph)).getKeyHash(),
						RDFSubject.STOP_KEY, RDFPredicate.STOP_KEY, RDFObject.END_STOP_KEY);
            }
        } else {
            Scan scan = HalyardTableUtils.scan(null, null);
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, sourceTableName.toBytes());
            HalyardTableUtils.appendSaltedScans(scan, scans);
        }
        TableMapReduceUtil.initTableMapperJob(
                scans,
                StatsMapper.class,
                ImmutableBytesWritable.class,
                LongWritable.class,
                job);
        job.setPartitionerClass(StatsPartitioner.class);
        job.setReducerClass(StatsReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        if (job.waitForCompletion(true)) {
            LOG.info("Stats Generation Completed..");
            return 0;
        }
        return -1;
    }
}
