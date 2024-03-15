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

import com.google.common.collect.Sets;
import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.model.TermRole;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.repository.HBaseRepository;
import com.msd.gin.halyard.repository.HBaseRepositoryConnection;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;
import com.msd.gin.halyard.sail.HalyardStatsBasedStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce tool providing statistics about a Halyard dataset. Statistics about a dataset are reported in RDF using the VOID ontology. These statistics can be useful
 * to summarize a graph and it implicitly shows how the subjects, predicates and objects are used. In the absence of schema information this information can be vital.
 * @author Adam Sotona (MSD)
 */
public final class HalyardStats extends AbstractHalyardTool {
    private static final Logger LOG = LoggerFactory.getLogger(HalyardStats.class);

    private static final String TOOL_NAME = "stats";

    private static final String TARGET = confProperty(TOOL_NAME, "target");
    private static final String GRAPH_THRESHOLD = confProperty(TOOL_NAME, "graph-threshold");
    private static final String PARTITION_THRESHOLD = confProperty(TOOL_NAME, "partition-threshold");
    private static final String SUBJECT_PARTITION_THRESHOLD = confProperty(TOOL_NAME, "subject-partition-threshold");
    private static final String PROPERTY_PARTITION_THRESHOLD = confProperty(TOOL_NAME, "property-partition-threshold");
    private static final String OBJECT_PARTITION_THRESHOLD = confProperty(TOOL_NAME, "object-partition-threshold");
    private static final String CLASS_PARTITION_THRESHOLD = confProperty(TOOL_NAME, "class-partition-threshold");
    private static final String STATS_GRAPH = confProperty(TOOL_NAME, "stats-graph");
    private static final String NAMED_GRAPH_PROPERTY = confProperty(TOOL_NAME, "named-graph");
    private static final String TIMESTAMP_PROPERTY = confProperty(TOOL_NAME, "timestamp");

    private static final long DEFAULT_GRAPH_THRESHOLD = 1000;
    private static final long DEFAULT_PARTITION_THRESHOLD = 5000;

	enum Counters {
		REMOVED_STATEMENTS,
	}

	enum DefaultGraphCounters {
		TRIPLES,
		DISTINCT_SUBJECTS,
		PROPERTIES,
		DISTINCT_OBJECTS,
		CLASSES,
	}

	static final class HashTracker {
		final int offset;
		final int len;
		final int end;
		byte[] lastHash;

		HashTracker(int offset, int len) {
			this.offset = offset;
			this.len = len;
			this.end = offset + len;
			this.lastHash = new byte[0];
		}

		boolean equals(byte[] key) {
            if (!Arrays.equals(key, offset, end, lastHash, 0, lastHash.length)) {
            	if (lastHash.length != len) {
            		lastHash = new byte[len];
            	}
            	System.arraycopy(key, offset, lastHash, 0, len);
            	return false;
            } else {
            	return true;
            }
		}
    }

    static final class StatsMapper extends RdfTableMapper<ImmutableBytesWritable, LongWritable>  {
        private static final long STATUS_UPDATE_INTERVAL = 100000L;
        private static final IRI DEFAULT_GRAPH_NODE = HALYARD.STATS_ROOT_NODE;

        final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
        final LongWritable outputValue = new LongWritable();
        ByteBuffer bb = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
        HalyardStatsBasedStatementPatternCardinalityCalculator.PartitionIriTransformer partitionIriTransformer;
        IRI statsContext;
        Set<String> namedGraphContexts;
        StatementIndex<?,?,?,?> lastIndex;
        long counter = 0L;
        boolean update;
        long timestamp;

        Resource lastCtx;
        Resource graph = DEFAULT_GRAPH_NODE;
        long triples, distinctSubjects, properties, distinctObjects, classes, removedStmts;
        long distinctIRIReferenceSubjects, distinctIRIReferenceObjects, distinctBlankNodeObjects, distinctBlankNodeSubjects, distinctLiterals;
        long distinctTripleSubjects, distinctTripleObjects;
        Value rdfClass;
        IRI subsetType;
		Value subsetId;
		Set<Value> lastSubsetIds;
		HashTracker hashTracker;
        long subsetDistincts;
        Set<Value> lastSubsetDistincts;
        IRI subsetDistinctType;
		HashTracker subhashTracker;
        long setThreshold, setCounter, subsetThreshold, subsetCounter;
        long instanceOfCounter, classPartitionThreshold;
		Map<IRI,Long> partitionThresholds;
        HBaseSail sail;
		HBaseSailConnection sailConn;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            openKeyspace(conf, conf.get(SOURCE_NAME_PROPERTY), conf.get(SNAPSHOT_PATH_PROPERTY));
            partitionIriTransformer = HalyardStatsBasedStatementPatternCardinalityCalculator.createPartitionIriTransformer(rdfFactory);
            update = (conf.get(TARGET) == null);
            timestamp = conf.getLong(TIMESTAMP_PROPERTY, System.currentTimeMillis());
            setThreshold = conf.getLong(GRAPH_THRESHOLD, DEFAULT_GRAPH_THRESHOLD);
            partitionThresholds = getPartitionThresholds(conf);
            classPartitionThreshold = partitionThresholds.get(VOID.CLASS);
            statsContext = vf.createIRI(conf.get(STATS_GRAPH));
            String[] namedGraphs = getStrings(conf, NAMED_GRAPH_PROPERTY);
            if (namedGraphs.length > 0) {
            	namedGraphContexts = new HashSet<>(namedGraphs.length + 1);
	            for (String g : namedGraphs) {
	            	namedGraphContexts.add(g);
	            }
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

        private boolean isIncludedGraphContext(String graphContext) {
        	return (namedGraphContexts == null) || namedGraphContexts.contains(graphContext);
        }

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result value, Context output) throws IOException, InterruptedException {
        	byte[] key = rowKey.get();
            StatementIndex<?,?,?,?> index = stmtIndices.toIndex(key[rowKey.getOffset()]);
            if (index != lastIndex) {
            	lastIndex = index;
            	lastCtx = null;
            	int hashLen, subhashLen;
                switch (index.getName()) {
                    case SPO:
                    case CSPO:
                        hashLen = index.getRole(TermRole.SUBJECT).keyHashSize();
                        subhashLen = index.getRole(TermRole.PREDICATE).keyHashSize();
                        subsetType = VOID_EXT.SUBJECT;
                        subsetDistinctType = VOID.PROPERTIES;
                        break;
                    case POS:
                    case CPOS:
                        hashLen = index.getRole(TermRole.PREDICATE).keyHashSize();
                        subhashLen = index.getRole(TermRole.OBJECT).keyHashSize();
                        subsetType = VOID.PROPERTY;
                        subsetDistinctType = VOID.DISTINCT_OBJECTS;
                        break;
                    case OSP:
                    case COSP:
                        hashLen = index.getRole(TermRole.OBJECT).keyHashSize();
                        subhashLen = index.getRole(TermRole.SUBJECT).keyHashSize();
                        subsetType = VOID_EXT.OBJECT;
                        subsetDistinctType = VOID.DISTINCT_SUBJECTS;
                        break;
                    default:
                        throw new IOException("Unknown index #" + index);
                }
                subsetThreshold = partitionThresholds.get(subsetType);
            	int offset = index.getName().isQuadIndex() ? 1 + index.getRole(TermRole.CONTEXT).keyHashSize() : 1;
            	hashTracker = new HashTracker(offset, hashLen);
            	subhashTracker = new HashTracker(hashTracker.end, subhashLen);
            }

            if (!hashTracker.equals(key)) {
            	lastSubsetIds = new HashSet<>();
            	lastSubsetDistincts = new HashSet<>();
            }

            if (!subhashTracker.equals(key)) {
            	lastSubsetDistincts = new HashSet<>();
            }

            Statement[] stmts = stmtIndices.parseStatements(null, null, null, null, value, vf);
            for (Statement stmt : stmts) {
            	Resource ctx = index.getName().isQuadIndex() ? stmt.getContext() : DEFAULT_GRAPH_NODE;
            	if (!ctx.equals(lastCtx)) {
            		lastCtx = ctx;
            		lastSubsetIds = new HashSet<>();
            		reset(output);
                    rdfClass = null;
            		graph = ctx;
            	}
            	if (graph.equals(statsContext) && (index.getName() == StatementIndex.Name.CSPO) && update) {
            		String stmtStatsNode;
            		String subj = stmt.getSubject().stringValue();
            		if (partitionIriTransformer.isPartitionIri(subj)) {
            			stmtStatsNode = partitionIriTransformer.getGraph(stmt.getSubject());
            		} else {
            			stmtStatsNode = subj;
            		}
                    if (isIncludedGraphContext(stmtStatsNode)) {
						getConnection(output).removeSystemStatement(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), timestamp);
                        removedStmts++;
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
	                			IRI pred = stmt.getPredicate();
	                			if (lastSubsetDistincts.add(pred)) {
	                				subsetDistincts++;
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
                    			Value obj = stmt.getObject();
	                			if (lastSubsetDistincts.add(obj)) {
	                				subsetDistincts++;
	                			}
	                    		if (RDF.TYPE.equals(pred)) {
	                    			if (!obj.equals(rdfClass)) {
	                    				resetClass(output);
		                    			rdfClass = obj;
		                    			classes++;
	                    			}
	                    			instanceOfCounter++;
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
                    			Value subj = stmt.getSubject();
	                			if (lastSubsetDistincts.add(subj)) {
	                				subsetDistincts++;
	                			}
		                	}
		                    break;
		                default:
		                    throw new AssertionError("Unknown index #" + index);
		            }
	                setCounter++;
	                subsetCounter++;
            	}
            }

            output.progress();
            if ((counter++ % STATUS_UPDATE_INTERVAL) == 0) {
				output.setStatus(MessageFormat.format("reg:{0} {1} t:{2} s:{3} p:{4} o:{5} c:{6} r:{7}", index, counter, triples, distinctSubjects, properties, distinctObjects, classes, removedStmts));
				LOG.info("StmtIndex:{} counter:{} triples:{} distinctSubjs:{} properties:{} distinctObjs:{} classes:{} removed:{}", index, counter, triples, distinctSubjects, properties, distinctObjects, classes, removedStmts);
            }
        }

        private void report(Context output, IRI property, long count) throws IOException, InterruptedException {
        	report(output, property, null, null, count);
        }

        /**
         * Reports a count.
         * @param output context
         * @param property property
         * @param partitionId if count is for a partition else null
         * @param subsetProperty subset property
         * @param count count
         * @throws IOException
         * @throws InterruptedException
         */
        private void report(Context output, IRI property, Value partitionId, IRI subsetProperty, long count) throws IOException, InterruptedException {
            if (count > 0 && isIncludedGraphContext(graph.stringValue())) {
            	writeStat(output, graph, property, partitionId, subsetProperty, count);
            }
        }

        private void writeStat(Context output, Resource graph, IRI property, Value partitionId, IRI subsetProperty, long count) throws IOException, InterruptedException {
        	ValueIO.Writer writer = rdfFactory.valueWriter;
        	bb.clear();
        	bb = writer.writeValueWithSizeHeader(graph, bb, Short.BYTES);
        	if (!VOID.TRIPLES.equals(property)) { // use null for void:triples so that it is the first property
            	bb = writer.writeValueWithSizeHeader(property, bb, Short.BYTES);
                if (partitionId != null) {
					bb = writer.writeValueWithSizeHeader(partitionId, bb, Short.BYTES);
					if (!VOID.TRIPLES.equals(subsetProperty) && !VOID.ENTITIES.equals(subsetProperty)) { // use null to be first property
						bb = writer.writeTo(subsetProperty, bb);
					}
                }
        	}
			bb.flip();
            outputKey.set(bb.array(), bb.arrayOffset(), bb.limit());
            outputValue.set(count);
            output.write(outputKey, outputValue);
        }

        private void reset(Context output) throws IOException, InterruptedException {
			assert setCounter <= triples;
			assert distinctSubjects <= triples;
			assert properties <= triples;
			assert distinctObjects <= triples;
			assert classes < distinctObjects;
			assert distinctBlankNodeObjects < distinctObjects;
			assert distinctBlankNodeSubjects < distinctSubjects;
			assert distinctIRIReferenceObjects < distinctObjects;
			assert distinctIRIReferenceSubjects < distinctSubjects;
			assert distinctLiterals < distinctObjects;
			assert distinctTripleObjects < distinctObjects;
			assert distinctTripleSubjects < distinctSubjects;
            if (graph == DEFAULT_GRAPH_NODE || setCounter >= setThreshold) {
                report(output, VOID.TRIPLES, triples);
                report(output, VOID.DISTINCT_SUBJECTS, distinctSubjects);
                report(output, VOID.PROPERTIES, properties);
                report(output, VOID.DISTINCT_OBJECTS, distinctObjects);
                report(output, VOID.CLASSES, classes);
                report(output, VOID_EXT.DISTINCT_IRI_REFERENCE_OBJECTS, distinctIRIReferenceObjects);
                report(output, VOID_EXT.DISTINCT_IRI_REFERENCE_SUBJECTS, distinctIRIReferenceSubjects);
                report(output, VOID_EXT.DISTINCT_BLANK_NODE_OBJECTS, distinctBlankNodeObjects);
                report(output, VOID_EXT.DISTINCT_BLANK_NODE_SUBJECTS, distinctBlankNodeSubjects);
                report(output, VOID_EXT.DISTINCT_LITERALS, distinctLiterals);
                report(output, VOID_EXT.DISTINCT_TRIPLE_OBJECTS, distinctTripleObjects);
                report(output, VOID_EXT.DISTINCT_TRIPLE_SUBJECTS, distinctTripleSubjects);
            } else {
                report(output, SD.NAMED_GRAPH_PROPERTY, 1L);
            }

			if (graph == DEFAULT_GRAPH_NODE) {
				output.getCounter(DefaultGraphCounters.TRIPLES).increment(triples);
				output.getCounter(DefaultGraphCounters.DISTINCT_SUBJECTS).increment(distinctSubjects);
				output.getCounter(DefaultGraphCounters.PROPERTIES).increment(properties);
				output.getCounter(DefaultGraphCounters.DISTINCT_OBJECTS).increment(distinctObjects);
				output.getCounter(DefaultGraphCounters.CLASSES).increment(classes);
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
			assert subsetDistincts <= subsetCounter;
            if (subsetCounter >= subsetThreshold) {
                report(output, subsetType, subsetId, VOID.TRIPLES, subsetCounter);
                report(output, subsetType, subsetId, subsetDistinctType, subsetDistincts);
            }
            subsetCounter = 0;
            subsetDistincts = 0;
            lastSubsetDistincts = new HashSet<>();
            resetClass(output);
        }

        private void resetClass(Context output) throws IOException, InterruptedException {
            if (instanceOfCounter >= classPartitionThreshold) {
            	report(output, VOID.CLASS, rdfClass, VOID.ENTITIES, instanceOfCounter);
            }
            instanceOfCounter = 0;
        }

        @Override
        protected void cleanup(Context output) throws IOException, InterruptedException {
        	reset(output);

			output.getCounter(Counters.REMOVED_STATEMENTS).increment(removedStmts);

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

    static final class StatsCombiner extends Reducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable, LongWritable> {
        final LongWritable outputValue = new LongWritable();

        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values) {
                count += val.get();
            }
            outputValue.set(count);
            context.write(key, outputValue);
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

    static final class StatsReducer extends RdfReducer<ImmutableBytesWritable, LongWritable, NullWritable, NullWritable> {
        private static final long STATUS_UPDATE_INTERVAL = 1000L;

        final Map<IRI,IRI> partitionPredicates = HalyardStatsBasedStatementPatternCardinalityCalculator.createPartitionPredicateMapping();
        OutputStream out;
        RDFWriter writer;
        IRI statsGraphContext;
        long timestamp;
        HBaseRepository repo;
        HBaseRepositoryConnection conn;
        long removed = 0, added = 0;
        HalyardStatsBasedStatementPatternCardinalityCalculator.PartitionIriTransformer partitionIriTransformer;
        IRI datePredicate;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            openKeyspace(conf, conf.get(SOURCE_NAME_PROPERTY), conf.get(SNAPSHOT_PATH_PROPERTY));
            partitionIriTransformer = HalyardStatsBasedStatementPatternCardinalityCalculator.createPartitionIriTransformer(rdfFactory);
            timestamp = conf.getLong(TIMESTAMP_PROPERTY, System.currentTimeMillis());
            statsGraphContext = vf.createIRI(conf.get(STATS_GRAPH));
            String targetUrl = conf.get(TARGET);
            if (targetUrl == null) {
                HBaseSail targetSail = new HBaseSail(conf, conf.get(SOURCE_NAME_PROPERTY), false, 0, true, 0, null, null);
                repo = new HBaseRepository(targetSail);
                repo.init();
				conn = repo.getConnection();
				conn.setNamespace(SD.PREFIX, SD.NAMESPACE);
				conn.setNamespace(VOID.PREFIX, VOID.NAMESPACE);
				conn.setNamespace(DCTERMS.PREFIX, DCTERMS.NAMESPACE);
				conn.setNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
				conn.setNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
				datePredicate = DCTERMS.MODIFIED;
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
                writer.handleNamespace(DCTERMS.PREFIX, DCTERMS.NAMESPACE);
                writer.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
                writer.handleNamespace(HALYARD.PREFIX, HALYARD.NAMESPACE);
				datePredicate = DCTERMS.CREATED;
            }

            if (conf.get(NAMED_GRAPH_PROPERTY) == null) {
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, VOID.DATASET);
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.DATASET);
                writeStatement(HALYARD.STATS_ROOT_NODE, RDF.TYPE, SD.GRAPH_CLASS);
                writeStatement(HALYARD.STATS_ROOT_NODE, SD.DEFAULT_GRAPH, HALYARD.STATS_ROOT_NODE);
                writeStatement(HALYARD.STATS_ROOT_NODE, datePredicate, vf.createLiteral(new Date(timestamp)));
                Map<IRI,Long> partitionThresholds = getPartitionThresholds(conf);
                writeStatement(HALYARD.STATS_ROOT_NODE, VOID_EXT.SUBJECT_PARTITION_THRESHOLD, vf.createLiteral(partitionThresholds.get(VOID_EXT.SUBJECT)));
                writeStatement(HALYARD.STATS_ROOT_NODE, VOID_EXT.PROPERTY_PARTITION_THRESHOLD, vf.createLiteral(partitionThresholds.get(VOID.PROPERTY)));
                writeStatement(HALYARD.STATS_ROOT_NODE, VOID_EXT.OBJECT_PARTITION_THRESHOLD, vf.createLiteral(partitionThresholds.get(VOID_EXT.OBJECT)));
                writeStatement(HALYARD.STATS_ROOT_NODE, VOID_EXT.CLASS_PARTITION_THRESHOLD, vf.createLiteral(partitionThresholds.get(VOID.CLASS)));
                long graphThreshold = conf.getLong(GRAPH_THRESHOLD, DEFAULT_GRAPH_THRESHOLD);
                writeStatement(HALYARD.STATS_ROOT_NODE, VOID_EXT.NAMED_GRAPH_THRESHOLD, vf.createLiteral(graphThreshold));
            }
        }

        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values) {
                count += val.get();
            }

        	ValueIO.Reader reader = rdfFactory.valueReader;
        	ByteBuffer bb = ByteBuffer.wrap(key.get(), key.getOffset(), key.getLength());
        	IRI graph = (IRI) reader.readValueWithSizeHeader(bb, vf, Short.BYTES);
        	IRI predicate = bb.hasRemaining() ? (IRI) reader.readValueWithSizeHeader(bb, vf, Short.BYTES) : VOID.TRIPLES;
            Value partitionId = bb.hasRemaining() ? reader.readValueWithSizeHeader(bb, vf, Short.BYTES) : null;
            IRI subsetPredicate = bb.hasRemaining() ? (IRI) reader.readValue(bb, vf) : (VOID.CLASS.equals(predicate) ? VOID.ENTITIES : VOID.TRIPLES);

            if (SD.NAMED_GRAPH_PROPERTY.equals(predicate)) { //workaround to at least count all small named graph that are below the threshold
                writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, graph);
            } else {
                IRI statsNode = graph;
                // VOID.TRIPLES is always the first predicate
                if (!HALYARD.STATS_ROOT_NODE.equals(graph) && VOID.TRIPLES.equals(predicate)) {
                    writeStatement(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, statsNode);
                    writeStatement(statsNode, SD.NAME, graph);
                    writeStatement(statsNode, SD.GRAPH_PROPERTY, statsNode);
                    writeStatement(statsNode, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
                    writeStatement(statsNode, RDF.TYPE, SD.GRAPH_CLASS);
                    writeStatement(statsNode, RDF.TYPE, VOID.DATASET);
                    writeStatement(statsNode, datePredicate, vf.createLiteral(new Date(timestamp)));
                }
                Literal countLiteral = vf.createLiteral(count);
                if (partitionId != null) {
					IRI subsetNode = vf.createIRI(partitionIriTransformer.apply(statsNode, predicate, partitionId));
	                // VOID.TRIPLES/VOID.ENTITIES is always the first subpredicate
					if (VOID.TRIPLES.equals(subsetPredicate) || VOID.ENTITIES.equals(subsetPredicate)) {
	                    writeStatement(statsNode, partitionPredicates.get(predicate), subsetNode);
	                    writeStatement(subsetNode, RDF.TYPE, VOID.DATASET);
						writeStatement(subsetNode, predicate, partitionId);
					}
                    writeStatement(subsetNode, subsetPredicate, countLiteral);
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
                ((HBaseSailConnection) conn.getSailConnection()).addSystemStatement(subj, pred, obj, statsGraphContext, timestamp);
            }
            if (writer != null) {
                writer.handleStatement(vf.createStatement(subj, pred, obj, statsGraphContext));
            }
            added++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] namedGraphs = getStrings(conf, NAMED_GRAPH_PROPERTY);
			if (namedGraphs.length > 0 && conn != null) {
				// approximately update default graph with any new partitions
				LOG.info("Updating default graph statistics");
				// ensure distinct namedGraphs
				String ngValues = Sets.newHashSet(namedGraphs).stream().map(ng -> "<" + NTriplesUtil.escapeString(ng) + ">").reduce("", (x,y) -> x + " " + y);
				String newStatsQuery = ""
					+ "PREFIX sd: <"+SD.NAMESPACE+">"
					+ "PREFIX void: <"+VOID.NAMESPACE+">"
					+ "PREFIX void_ext: <"+VOID_EXT.NAMESPACE+">"
					+ "PREFIX halyard: <"+HALYARD.NAMESPACE+">"
					+ "CONSTRUCT {"
					+ " ?dgs ?partitionType ?v; ?stat ?total ."
					+ "}\n"
					+ "FROM " + NTriplesUtil.toNTriplesString(statsGraphContext) + "\n"
					+ "WHERE {"
					+ " {"
					+ "SELECT ?partitionType ?v ?stat (SUM(?x) as ?total)"
					+ "  {\n"
					+ " VALUES ?ng {" + ngValues + "}\n"
					+ " VALUES (?partition ?partitionType) {"
					+ " (void:propertyPartition void:property)"
					+ " (void_ext:subjectPartition void_ext:subject)"
					+ " (void_ext:objectPartition void_ext:object)"
					+ " (void:classPartition void:class)"
					+ " }\n"
					+ " ?ng ^sd:name/sd:graph [ ?partition [ a void:Dataset; ?partitionType ?v; ?stat ?x ] ] ."
					+ " FILTER (isNumeric(?x))"
					+ "  } GROUP BY ?partitionType ?v ?stat"
					+ " }"
					+ "BIND(halyard:datasetIRI(?statsRootNode, ?partitionType, ?v) as ?dgs)"
					+ "FILTER NOT EXISTS { ?dgs ?partitionType ?v }"
					+ "}";
				GraphQuery query = conn.prepareGraphQuery(newStatsQuery);
				query.setBinding("statsRootNode", HALYARD.STATS_ROOT_NODE);
				try (GraphQueryResult res = query.evaluate()) {
					for (Statement stmt : res) {
						writeStatement(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
					}
				}
			}

        	if (conn != null) {
				conn.close();
				conn = null;
        	}
			if (repo != null) {
				repo.shutDown();
				repo = null;
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

    private static Map<IRI,Long> getPartitionThresholds(Configuration conf) {
    	Map<IRI,Long> thresholds = new HashMap<>();
        long defaultSubsetThreshold = conf.getLong(PARTITION_THRESHOLD, DEFAULT_PARTITION_THRESHOLD);
        thresholds.put(VOID_EXT.SUBJECT, conf.getLong(SUBJECT_PARTITION_THRESHOLD, defaultSubsetThreshold));
        thresholds.put(VOID.PROPERTY, conf.getLong(PROPERTY_PARTITION_THRESHOLD, defaultSubsetThreshold));
        thresholds.put(VOID_EXT.OBJECT, conf.getLong(OBJECT_PARTITION_THRESHOLD, defaultSubsetThreshold));
        thresholds.put(VOID.CLASS, conf.getLong(CLASS_PARTITION_THRESHOLD, defaultSubsetThreshold));
        return thresholds;
    }


    public HalyardStats() {
        super(
            TOOL_NAME,
            "Halyard Stats is a MapReduce application that calculates dataset statistics and stores them in the named graph within the dataset or exports them into a file. The generated statistics are described by the VoID vocabulary, its extensions, and the SPARQL 1.1 Service Description.",
            "Example: halyard stats -s my_dataset [-g 'http://whatever/mystats'] [-t hdfs:/my_folder/my_stats.trig]");
        addOption("s", "source-dataset", "dataset_table", SOURCE_NAME_PROPERTY, "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-file", "target_url", TARGET, "Optional target file to export the statistics (instead of update) hdfs://<path>/<file_name>[{0}].<RDF_ext>[.<compression>]", false, true);
        addOption("R", "graph-threshold", "size", GRAPH_THRESHOLD, "Optional minimal size of a named graph to calculate statistics for (default is 1000)", false, true);
        addOption("r", "partition-threshold", "size", PARTITION_THRESHOLD, "Optional minimal size of a graph partition to calculate statistics for (default is 1000)", false, true);
        addOption("g", "named-graph", "named_graph", NAMED_GRAPH_PROPERTY, "Optional restrict stats calculation to the given named graph only", false, false);
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
        configureStrings(cmd, 'g', null);
        configureIRI(cmd, 'o', HALYARD.STATS_GRAPH_CONTEXT.stringValue());
        configureString(cmd, 'u', null);
        configureLong(cmd, 'R', DEFAULT_GRAPH_THRESHOLD);
        configureLong(cmd, 'r', DEFAULT_PARTITION_THRESHOLD);
        configureLong(cmd, 'e', System.currentTimeMillis());
        String source = getConf().get(SOURCE_NAME_PROPERTY);
        String target = getConf().get(TARGET);
        String statsGraph = getConf().get(STATS_GRAPH);
        List<String> namedGraphs = Arrays.asList(getStrings(getConf(), NAMED_GRAPH_PROPERTY));
        String snapshotPath = getConf().get(SNAPSHOT_PATH_PROPERTY);

        if (namedGraphs.size() == 1) {
    		String namedGraph = namedGraphs.get(0);
    		if (namedGraph.equals("CREATED")) {
    			namedGraphs = getNewlyCreatedGraphs(source, statsGraph);
    			if (namedGraphs.isEmpty()) {
    	            LOG.info("No new named graphs found.");
    				return 0;
    			}
    			LOG.info("Found {} new named graphs: {}", namedGraphs.size(), namedGraphs);
    			setStrings(getConf(), NAMED_GRAPH_PROPERTY, namedGraphs);
    		}
    	}

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
        Keyspace keyspace = getKeyspace(source, snapshotPath);
        try {
       		rdfFactory = loadRDFFactory(keyspace);
		} finally {
			keyspace.close();
		}
        StatementIndices indices = new StatementIndices(getConf(), rdfFactory);
        List<Scan> scans;
        if (!namedGraphs.isEmpty()) {  //restricting stats to scan given graph context only
            scans = new ArrayList<>(3*namedGraphs.size() + 1);
            ValueFactory vf = new IdValueFactory(rdfFactory);
            for (String namedGraph : namedGraphs) {
	            RDFContext rdfGraphCtx = rdfFactory.createContext(vf.createIRI(namedGraph));
	            scans.add(indices.getCSPOIndex().scan(rdfGraphCtx));
	            scans.add(indices.getCPOSIndex().scan(rdfGraphCtx));
	            scans.add(indices.getCOSPIndex().scan(rdfGraphCtx));
            }
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
        // if writing to files then partition a graph per file
        if (target != null) {
        	job.setPartitionerClass(StatsPartitioner.class);
        }
        job.setCombinerClass(StatsCombiner.class);
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

    private List<String> getNewlyCreatedGraphs(String table, String statsGraph) {
        Repository repo = new HBaseRepository(new HBaseSail(getConf(), table, false, 0, true, 0, null, null));
        repo.init();
        List<String> graphs = new ArrayList<>();
        try {
			try (RepositoryConnection conn = repo.getConnection()) {
				TupleQuery modifiedQuery = conn.prepareTupleQuery(
						"PREFIX dc: <"+DCTERMS.NAMESPACE+">"
						+ "select ?modified where {"
						+ "graph ?statsContext { ?statsRootNode dc:modified ?modified}"
						+ "}"
						+ "order by desc(?modified)"
						+ "limit 1");
				modifiedQuery.setBinding("statsRootNode", HALYARD.STATS_ROOT_NODE);
				modifiedQuery.setBinding("statsContext", repo.getValueFactory().createIRI(statsGraph));
				Value lastUpdated;
				try (TupleQueryResult tqr = modifiedQuery.evaluate()) {
					if (tqr.hasNext()) {
						lastUpdated = tqr.next().getValue("modified");
						LOG.info("Stats last modified: {}", lastUpdated);
					} else {
						lastUpdated = null;
					}
				}
				if (lastUpdated != null) {
					TupleQuery graphQuery = conn.prepareTupleQuery(
							"PREFIX sd: <"+SD.NAMESPACE+">"
							+ "PREFIX void: <"+VOID.NAMESPACE+">"
							+ "PREFIX dc: <"+DCTERMS.NAMESPACE+">"
							+ "select distinct ?graph where {"
							+ "?graph ^sd:name/sd:graph/dc:created ?t . filter(?t > ?lastUpdated)"
							+ "}");
					graphQuery.setBinding("lastUpdated", lastUpdated);
					try (TupleQueryResult tqr = graphQuery.evaluate()) {
						for (BindingSet bs : tqr) {
							IRI graph = (IRI) bs.getValue("graph");
							graphs.add(graph.stringValue());
						}
					}
				}
			}
        } finally {
        	repo.shutDown();
        }
        return graphs;
    }
}
