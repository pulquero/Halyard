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

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.RDFRole;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;

/**
 * MapReduce tool providing statistics about a Halyard dataset. Statistics about a dataset are reported in RDF using the VOID ontology. These statistics can be useful
 * to summarize a graph and it implicitly shows how the subjects, predicates and objects are used. In the absence of schema information this information can be vital.
 * @author Adam Sotona (MSD)
 */
public final class HalyardStats extends AbstractHalyardTool {
	private static final String TOOL_NAME = "stats";

    private static final String TARGET = confProperty(TOOL_NAME, "target");
    private static final String GRAPH_THRESHOLD = confProperty(TOOL_NAME, "graph-threshold");
    private static final String PARTITION_THRESHOLD = confProperty(TOOL_NAME, "partition-threshold");
    private static final String STATS_GRAPH = confProperty(TOOL_NAME, "stats-graph");
    private static final String NAMED_GRAPH_PROPERTY = confProperty(TOOL_NAME, "named-graph");
    private static final String TIMESTAMP_PROPERTY = confProperty(TOOL_NAME, "timestamp");

    private static final long DEFAULT_THRESHOLD = 1000;

    enum Counters {
		KEYS
	}

    static final class StatsMapper extends RdfTableMapper<ImmutableBytesWritable, LongWritable>  {
        private static final long STATUS_UPDATE_INTERVAL = 100000L;
        private static final IRI DEFAULT_GRAPH_NODE = HALYARD.STATS_ROOT_NODE;

        final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
        final LongWritable outputValue = new LongWritable();
        ByteBuffer bb = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
        IRI statsContext, namedGraphContext;
        StatementIndex<?,?,?,?> lastIndex;
        long counter = 0L;
        boolean update;
        long timestamp;

        Resource graph = DEFAULT_GRAPH_NODE, lastGraph;
        long triples, distinctSubjects, properties, distinctObjects, classes, removed;
        long distinctIRIReferenceSubjects, distinctIRIReferenceObjects, distinctBlankNodeObjects, distinctBlankNodeSubjects, distinctLiterals;
        long distinctTripleSubjects, distinctTripleObjects;
        Value rdfClass;
        IRI subsetType;
		Value subsetId;
		Set<Value> lastSubsetIds;
		byte[] lastHash;
		int hashOffset;
		int hashLen;
        long setThreshold, setCounter, subsetThreshold, subsetCounter;
        HBaseSail sail;
		HBaseSailConnection sailConn;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            openKeyspace(conf, conf.get(SOURCE_NAME_PROPERTY), conf.get(SNAPSHOT_PATH_PROPERTY));
            update = (conf.get(TARGET) == null);
            timestamp = conf.getLong(TIMESTAMP_PROPERTY, System.currentTimeMillis());
            setThreshold = conf.getLong(GRAPH_THRESHOLD, DEFAULT_THRESHOLD);
            subsetThreshold = conf.getLong(PARTITION_THRESHOLD, DEFAULT_THRESHOLD);
            ValueFactory vf = IdValueFactory.INSTANCE;
            statsContext = vf.createIRI(conf.get(STATS_GRAPH));
            String namedGraph = conf.get(NAMED_GRAPH_PROPERTY);
            if (namedGraph != null) {
            	namedGraphContext = vf.createIRI(namedGraph);
            }
        }

        private HBaseSailConnection getConnection(Context output) {
            if (sail == null) {
                Configuration conf = output.getConfiguration();
                sail = new HBaseSail(conf, conf.get(SOURCE_NAME_PROPERTY), false, 0, true, 0, null, null);
                sail.init();
            }
            if (sailConn == null) {
            	sailConn = sail.getConnection();
            }
            return sailConn;
        }

        private boolean matchingGraphContext(Resource subject) {
        	if (namedGraphContext == null || namedGraphContext.equals(subject)) {
        		return true;
        	}
        	String subjValue = subject.stringValue();
            String namedGraph = namedGraphContext.stringValue();
            return subjValue.startsWith(namedGraph + "_subject_")
                || subjValue.startsWith(namedGraph + "_property_")
                || subjValue.startsWith(namedGraph + "_object_");
        }

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result value, Context output) throws IOException, InterruptedException {
        	byte[] key = rowKey.get();
            StatementIndex<?,?,?,?> index = stmtIndices.toIndex(key[rowKey.getOffset()]);
            if (index != lastIndex) {
            	lastIndex = index;
            	lastHash = new byte[0];
            	lastGraph = null;
            	hashOffset = index.getName().isQuadIndex() ? 1 + index.getRole(RDFRole.Name.CONTEXT).keyHashSize() : 1;
                switch (index.getName()) {
                    case SPO:
                    case CSPO:
                        hashLen = index.getRole(RDFRole.Name.SUBJECT).keyHashSize();
                        subsetType = VOID_EXT.SUBJECT;
                        break;
                    case POS:
                    case CPOS:
                        hashLen = index.getRole(RDFRole.Name.PREDICATE).keyHashSize();
                        subsetType = VOID.PROPERTY;
                        break;
                    case OSP:
                    case COSP:
                        hashLen = index.getRole(RDFRole.Name.OBJECT).keyHashSize();
                        subsetType = VOID_EXT.OBJECT;
                        break;
                    default:
                        throw new IOException("Unknown region #" + index);
                }
            }

            if (!Arrays.equals(key, hashOffset, hashOffset + hashLen, lastHash, 0, lastHash.length)) {
            	if (lastHash.length != hashLen) {
            		lastHash = new byte[hashLen];
            	}
            	System.arraycopy(key, hashOffset, lastHash, 0, hashLen);
            	lastSubsetIds = new HashSet<>();
            }

            List<Statement> stmts = HalyardTableUtils.parseStatements(null, null, null, null, value, valueReader, stmtIndices);
            for (Statement stmt : stmts) {
            	Resource ctx = index.getName().isQuadIndex() ? stmt.getContext() : DEFAULT_GRAPH_NODE;
            	if (!ctx.equals(lastGraph)) {
            		lastGraph = ctx;
            		lastSubsetIds = new HashSet<>();
            		reset(output);
                    rdfClass = null;
            		graph = ctx;
            	}
            	if (update && index.getName() == StatementIndex.Name.CSPO && graph.equals(statsContext)) {
                    if (matchingGraphContext(stmt.getSubject())) {
						getConnection(output).removeSystemStatement(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), timestamp);
                        removed++;
                    }
            	} else {
		            switch (index.getName()) {
		                case SPO:
		                case CSPO:
		                	{
		                        Resource subj = stmt.getSubject();
		                        if (lastSubsetIds.add(subj)) {
			                        resetSubset(output);
			                        distinctSubjects++;
			                        if (subj.isIRI()) {
			                            distinctIRIReferenceSubjects++;
			                        } else if (subj.isTriple()) {
			                        	distinctTripleSubjects++;
			                        } else {
			                            distinctBlankNodeSubjects++;
			                        }
			                        subsetId = subj;
		                        }
		                		triples++;
		                	}
		                    break;
		                case POS:
		                case CPOS:
		                	{
	                			IRI pred = stmt.getPredicate();
		                        if (lastSubsetIds.add(pred)) {
	                				resetSubset(output);
			                        properties++;
			                        subsetId = pred;
	                			}
	                    		if (RDF.TYPE.equals(stmt.getPredicate())) {
	                    			Value obj = stmt.getObject();
	                    			if (!obj.equals(rdfClass)) {
		                    			rdfClass = obj;
		                    			classes++;
	                    			}
	                    		}
		                	}
		                    break;
		                case OSP:
		                case COSP:
		                	{
		                        Value obj = stmt.getObject();
		                        if (lastSubsetIds.add(obj)) {
		                        	resetSubset(output);
			                        distinctObjects++;
			                        if (obj.isIRI()) {
			                        	distinctIRIReferenceObjects++;
			                        } else if (obj.isTriple()) {
			                        	distinctTripleObjects++;
			                        } else if (obj.isBNode()) {
			                        	distinctBlankNodeObjects++;
			                        } else {
			                            distinctLiterals++;
			                        }
			                        subsetId = obj;
		                        }
		                	}
		                    break;
		                default:
		                    throw new AssertionError("Unknown region #" + index);
		            }
	                setCounter++;
	                subsetCounter++;
            	}
            }

            output.progress();
            if ((counter++ % STATUS_UPDATE_INTERVAL) == 0) {
                output.setStatus(MessageFormat.format("reg:{0} {1} t:{2} s:{3} p:{4} o:{5} c:{6} r:{7}", index, counter, triples, distinctSubjects, properties, distinctObjects, classes, removed));
            }
        }

        private void report(Context output, IRI property, Value partitionId, long count) throws IOException, InterruptedException {
            if (count > 0 && (namedGraphContext == null || namedGraphContext.equals(graph))) {
            	ValueIO.Writer writer = rdfFactory.streamWriter;
            	bb.clear();
            	bb = ValueIO.writeValue(graph, writer, bb, Short.BYTES);
            	bb = ValueIO.writeValue(property, writer, bb, Short.BYTES);
                if (partitionId != null) {
					bb = writer.writeTo(partitionId, bb);
                }
				bb.flip();
                outputKey.set(bb.array(), bb.arrayOffset(), bb.limit());
                outputValue.set(count);
                output.write(outputKey, outputValue);
            }
        }

		private void reset(Context output) throws IOException, InterruptedException {
            if (graph == DEFAULT_GRAPH_NODE || setCounter >= setThreshold) {
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
                report(output, VOID_EXT.DISTINCT_TRIPLE_OBJECTS, null, distinctTripleObjects);
                report(output, VOID_EXT.DISTINCT_TRIPLE_SUBJECTS, null, distinctTripleSubjects);
            } else {
                report(output, SD.NAMED_GRAPH_PROPERTY, null, 1);
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
            distinctTripleObjects = 0;
            distinctTripleSubjects = 0;
            resetSubset(output);
		}

		private void resetSubset(Context output) throws IOException, InterruptedException {
            if (subsetCounter >= subsetThreshold) {
                report(output, subsetType, subsetId, subsetCounter);
            }
            subsetCounter = 0;
        }

        @Override
        protected void cleanup(Context output) throws IOException, InterruptedException {
        	reset(output);
        	if (sailConn != null) {
        		sailConn.close();
        		sailConn = null;
        	}
            if (sail != null) {
				sail.shutDown();
                sail = null;
            }
            closeKeyspace();
        }

    }

    static final class StatsPartitioner extends Partitioner<ImmutableBytesWritable, LongWritable> {

    	@Override
        public int getPartition(ImmutableBytesWritable key, LongWritable value, int numPartitions) {
        	ByteBuffer buf = ByteBuffer.wrap(key.get(), key.getOffset(), key.getLength());
        	int graphBytesLen = buf.getShort();
        	byte[] graphBytes = new byte[graphBytesLen];
        	buf.get(graphBytes);
        	int hash = Arrays.hashCode(graphBytes);
            return Math.floorMod(hash, numPartitions);
        }
    }

    static final class StatsReducer extends RdfReducer<ImmutableBytesWritable, LongWritable, NullWritable, NullWritable>  {
        private static final long STATUS_UPDATE_INTERVAL = 1000L;

        OutputStream out;
        RDFWriter writer;
        Map<Resource, Boolean> graphs;
        IRI statsGraphContext;
        ValueFactory vf;
        long timestamp;
        HBaseSail sail;
		HBaseSailConnection conn;
        long removed = 0, added = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            openKeyspace(conf, conf.get(SOURCE_NAME_PROPERTY), conf.get(SNAPSHOT_PATH_PROPERTY));
            vf = IdValueFactory.INSTANCE;
            timestamp = conf.getLong(TIMESTAMP_PROPERTY, System.currentTimeMillis());
            statsGraphContext = vf.createIRI(conf.get(STATS_GRAPH));
            String targetUrl = conf.get(TARGET);
            if (targetUrl == null) {
                sail = new HBaseSail(conf, conf.get(SOURCE_NAME_PROPERTY), false, 0, true, 0, null, null);
                sail.init();
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
                if (!form.isPresent()) {
                	throw new IOException("Unsupported target file format extension: " + targetUrl);
                }
                writer = Rio.createWriter(form.get(), out);
                writer.startRDF();
                writer.handleNamespace(SD.PREFIX, SD.NAMESPACE);
                writer.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
                writer.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
                writer.handleNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
            }
            if (conf.get(NAMED_GRAPH_PROPERTY) == null) {
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, VOID.DATASET);
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.DATASET);
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.GRAPH_CLASS);
                writeStatement(HALYARD.STATS_ROOT_NODE, SD.DEFAULT_GRAPH, HALYARD.STATS_ROOT_NODE);
            }
            graphs = new WeakHashMap<>();
        }

        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values) {
                count += val.get();
            }

        	ValueIO.Reader reader = rdfFactory.streamReader;
        	ByteBuffer bb = ByteBuffer.wrap(key.get(), key.getOffset(), key.getLength());
        	Resource graph = (Resource) ValueIO.readValue(bb, reader, Short.BYTES);
        	IRI predicate = (IRI) ValueIO.readValue(bb, reader, Short.BYTES);
            Value partitionId = bb.hasRemaining() ? reader.readValue(bb) : null;

            if (SD.NAMED_GRAPH_PROPERTY.equals(predicate)) { //workaround to at least count all small named graph that are below the threshold
                writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graph);
            } else {
                Resource statsNode;
                if (HALYARD.STATS_ROOT_NODE.equals(graph)) {
                    statsNode = HALYARD.STATS_ROOT_NODE;
                } else {
                    statsNode = graph;
                    if (graphs.putIfAbsent(graph, Boolean.FALSE) == null) {
                        writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, statsNode);
                        writeStatement(statsNode, SD.NAME, statsNode);
                        writeStatement(statsNode, SD.GRAPH_PROPERTY, statsNode);
                        writeStatement(statsNode, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
                        writeStatement(statsNode, RDF.TYPE, SD.GRAPH_CLASS);
                        writeStatement(statsNode, RDF.TYPE, VOID.DATASET);
                    }
                }
                Literal countLiteral = vf.createLiteral(count);
                if (partitionId != null) {
					IRI subset = vf.createIRI(graph + "_" + predicate.getLocalName() + "_" + rdfFactory.id(partitionId));
                    writeStatement(statsNode, vf.createIRI(predicate + "Partition"), subset);
                    writeStatement(subset, RDF.TYPE, VOID.DATASET);
					writeStatement(subset, predicate, partitionId);
                    writeStatement(subset, VOID.TRIPLES, countLiteral);
                } else {
                    writeStatement(statsNode, predicate, countLiteral);
                }
                if ((added % STATUS_UPDATE_INTERVAL) == 0) {
                    context.setStatus(MessageFormat.format("statements removed: {0} added: {1}", removed, added));
                }
            }
        }

        private void writeStatement(Resource subj, IRI pred, Value obj) {
            if (conn != null) {
				conn.addSystemStatement(subj, pred, obj, statsGraphContext, timestamp);
            }
            if (writer != null) {
                writer.handleStatement(vf.createStatement(subj, pred, obj, statsGraphContext));
            }
            added++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (conn != null) {
				conn.close();
				conn = null;
            }
			if (sail != null) {
				sail.shutDown();
				sail = null;
            }
            if (writer != null) {
                writer.endRDF();
                writer = null;
            }
            if (out != null) {
                out.close();
                out = null;
            }
            closeKeyspace();
        }
    }

    public HalyardStats() {
        super(
            TOOL_NAME,
            "Halyard Stats is a MapReduce application that calculates dataset statistics and stores them in the named graph within the dataset or exports them into a file. The generated statistics are described by the VoID vocabulary, its extensions, and the SPARQL 1.1 Service Description.",
            "Example: halyard stats -s my_dataset [-g 'http://whatever/mystats'] [-t hdfs:/my_folder/my_stats.trig]");
        addOption("s", "source-dataset", "dataset_table", SOURCE_NAME_PROPERTY, "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-file", "target_url", TARGET, "Optional target file to export the statistics (instead of update)) hdfs://<path>/<file_name>[{0}].<RDF_ext>[.<compression>]", false, true);
        addOption("R", "graph-threshold", "size", GRAPH_THRESHOLD, "Optional minimal size of a named graph to calculate statistics for (default is 1000)", false, true);
        addOption("r", "partition-threshold", "size", PARTITION_THRESHOLD, "Optional minimal size of a graph partition to calculate statistics for (default is 1000)", false, true);
        addOption("g", "named-graph", "named_graph", NAMED_GRAPH_PROPERTY, "Optional restrict stats calculation to the given named graph only", false, true);
        addOption("o", "stats-named-graph", "target_graph", STATS_GRAPH, "Optional target named graph of the exported statistics (default value is '" + HALYARD.STATS_GRAPH_CONTEXT.stringValue() + "'), modification is recomended only for external export as internal Halyard optimizers expect the default value", false, true);
        addOption("u", "restore-dir", "restore_folder", SNAPSHOT_PATH_PROPERTY, "If specified then -s is a snapshot name and this is the restore folder on HDFS", false, true);
        addOption("e", "target-timestamp", "timestamp", TIMESTAMP_PROPERTY, "Optionally specify timestamp of stat statements (default is actual time of the operation)", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
    	if (cmd.hasOption('u') && !cmd.hasOption('t')) {
    		throw new MissingOptionException("Statistics cannot be written to a snapshot, please specify -t.");
    	}
        configureString(cmd, 's', null);
        configureString(cmd, 't', null);
        configureIRI(cmd, 'g', null);
        configureIRI(cmd, 'o', HALYARD.STATS_GRAPH_CONTEXT.stringValue());
        configureString(cmd, 'u', null);
        configureLong(cmd, 'R', DEFAULT_THRESHOLD);
        configureLong(cmd, 'r', DEFAULT_THRESHOLD);
        configureLong(cmd, 'e', System.currentTimeMillis());
        String source = getConf().get(SOURCE_NAME_PROPERTY);
        String target = getConf().get(TARGET);
        String statsGraph = getConf().get(STATS_GRAPH);
        String namedGraph = getConf().get(NAMED_GRAPH_PROPERTY);
        String snapshotPath = getConf().get(SNAPSHOT_PATH_PROPERTY);
        TableMapReduceUtil.addDependencyJarsForClasses(getConf(),
               NTriplesUtil.class,
               Rio.class,
               AbstractRDFHandler.class,
               RDFFormat.class,
               RDFParser.class,
               Table.class,
               HBaseConfiguration.class,
               AuthenticationProtos.class);
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardStats " + source + (target == null ? " update" : " -> " + target));
        if (snapshotPath != null) {
			FileSystem fs = CommonFSUtils.getRootDirFileSystem(getConf());
        	if (fs.exists(new Path(snapshotPath))) {
        		throw new IOException("Snapshot restore directory already exists");
        	}
        }
        job.setJarByClass(HalyardStats.class);
        TableMapReduceUtil.initCredentials(job);

        RDFFactory rdfFactory;
        Keyspace keyspace = HalyardTableUtils.getKeyspace(getConf(), source, snapshotPath);
        try {
        	try (KeyspaceConnection kc = keyspace.getConnection()) {
        		rdfFactory = RDFFactory.create(kc);
        	}
		} finally {
			keyspace.close();
		}
        StatementIndices indices = new StatementIndices(getConf(), rdfFactory);
        List<Scan> scans;
        if (namedGraph != null) {  //restricting stats to scan given graph context only
            scans = new ArrayList<>(4);
            ValueFactory vf = IdValueFactory.INSTANCE;
            RDFContext rdfGraphCtx = rdfFactory.createContext(vf.createIRI(namedGraph));
            scans.add(indices.getCSPOIndex().scan(rdfGraphCtx));
            scans.add(indices.getCPOSIndex().scan(rdfGraphCtx));
            scans.add(indices.getCOSPIndex().scan(rdfGraphCtx));
            if (target == null) {
                // add stats context to the scanned row ranges (when in update mode) to delete the related stats during MapReduce
				scans.add(indices.getCSPOIndex().scan(
					rdfFactory.createContext(statsGraph == null ? HALYARD.STATS_GRAPH_CONTEXT : vf.createIRI(statsGraph))
				));
            }
        } else {
            scans = Collections.singletonList(indices.scanAll());
        }
        keyspace.initMapperJob(
	        scans,
	        StatsMapper.class,
	        ImmutableBytesWritable.class,
	        LongWritable.class,
	        job);
        job.setPartitionerClass(StatsPartitioner.class);
        job.setReducerClass(StatsReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        try {
	        if (job.waitForCompletion(true)) {
	            LOG.info("Stats Generation completed.");
	            return 0;
	        } else {
	    		LOG.error("Stats Generation failed to complete.");
	            return -1;
	        }
        } finally {
        	keyspace.destroy();
        }
    }
}
