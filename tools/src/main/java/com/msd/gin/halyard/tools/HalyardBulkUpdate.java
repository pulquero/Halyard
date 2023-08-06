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

import static com.msd.gin.halyard.tools.HalyardBulkLoad.*;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.repository.HBaseUpdate;
import com.msd.gin.halyard.sail.ElasticSettings;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.eclipse.rdf4j.sail.SailException;

/**
 * Apache Hadoop MapReduce tool for performing SPARQL Graph construct queries and then bulk loading the results back into HBase. Essentially, batch process queries
 * and bulk load the results.
 * @author Adam Sotona (MSD)
 */
public final class HalyardBulkUpdate extends AbstractHalyardTool {
	private static final String TOOL_NAME = "bulkupdate";

    private static final String TABLE_NAME_PROPERTY = confProperty(TOOL_NAME, "table.name");
    private static final String STAGE_PROPERTY = confProperty(TOOL_NAME, "update.stage");
    private static final long STATUS_UPDATE_INTERVAL = 10000L;

    enum Counters {
		ADDED_STATEMENTS,
		REMOVED_STATEMENTS,
		ADDED_KVS,
		REMOVED_KVS
	}

    /**
     * Mapper class performing SPARQL Graph query evaluation and producing Halyard KeyValue pairs for HBase BulkLoad Reducers
     */
    public static final class SPARQLUpdateMapper extends Mapper<NullWritable, Void, ImmutableBytesWritable, KeyValue> {

        private String tableName;
        private long timestamp;
        private int stage;
        private String queryName;
        private final AtomicLong addedStmts = new AtomicLong();
        private final AtomicLong removedStmts = new AtomicLong();
        private final AtomicLong addedKvs = new AtomicLong();
        private final AtomicLong removedKvs = new AtomicLong();

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            tableName = conf.get(TABLE_NAME_PROPERTY);
            timestamp = conf.getLong(TIMESTAMP_PROPERTY, System.currentTimeMillis());
            stage = conf.getInt(STAGE_PROPERTY, 0);
            final QueryInputFormat.QueryInputSplit qis = (QueryInputFormat.QueryInputSplit)context.getInputSplit();
            final String query = qis.getQuery();
            queryName = qis.getQueryName();
            ParsedUpdate parsedUpdate = QueryParserUtil.parseUpdate(QueryLanguage.SPARQL, query, null);
            if (parsedUpdate.getUpdateExprs().size() <= stage) {
                context.setStatus("Nothing to execute in: " + queryName + " for stage #" + stage);
            } else {
                UpdateExpr ue = parsedUpdate.getUpdateExprs().get(stage);
                ParsedUpdate singleUpdate = new ParsedUpdate(parsedUpdate.getSourceString(), parsedUpdate.getNamespaces());
                singleUpdate.addUpdateExpr(ue);
                Dataset d = parsedUpdate.getDatasetMapping().get(ue);
                if (d != null) {
                    singleUpdate.map(ue, d);
                }
                context.setStatus("Execution of: " + queryName + " stage #" + stage);
				final HBaseSail sail = new HBaseSail(context.getConfiguration(), tableName, false, 0, true, 0, ElasticSettings.from(conf), new HBaseSail.Ticker() {
                    @Override
                    public void tick() {
                        context.progress();
                    }
				}, new HBaseSail.SailConnectionFactory() {
					@Override
					public HBaseSailConnection createConnection(HBaseSail sail) throws IOException {
						return new HBaseSailConnection(sail) {
							private final ImmutableBytesWritable rowKey = new ImmutableBytesWritable();

							@Override
							protected void evaluateInternal(Consumer<BindingSet> handler, TupleExpr tupleExpr, QueryEvaluationStep step) {
								LOG.info("Optimised query tree:\n{}", tupleExpr);
								HalyardEvaluationStatistics stats = sail.getStatistics();
								double estimate = stats.getCardinality(tupleExpr);
								super.evaluateInternal(next -> {
									qis.setProgress((float) ((double)tupleExpr.getResultSizeActual()/estimate));
									// notify of progress
									try {
										context.nextKeyValue();
									} catch (IOException | InterruptedException e) {
										throw new QueryEvaluationException(e);
									}
									handler.accept(next);
								}, tupleExpr, step);
								LOG.info("Execution statistics:\n{}", tupleExpr);
							}

							@Override
							protected int insertStatement(Resource subj, IRI pred, Value obj, Resource ctx, long timestamp) throws IOException {
								int insertedKvs = super.insertStatement(subj, pred, obj, ctx, timestamp);
								long _addedStmts = addedStmts.incrementAndGet();
								addedKvs.addAndGet(insertedKvs);
								if (_addedStmts % STATUS_UPDATE_INTERVAL == 0) {
									updateStatus(context);
								}
								return insertedKvs;
							}

							@Override
							protected void put(KeyValue kv) throws IOException {
								rowKey.set(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
								try {
									context.write(rowKey, kv);
								} catch (InterruptedException ex) {
									throw new IOException(ex);
								}
							}

							@Override
							protected int deleteStatement(Resource subj, IRI pred, Value obj, Resource ctx, long timestamp) throws IOException {
								int deletedKvs = super.deleteStatement(subj, pred, obj, ctx, timestamp);
								long _removedStmts = removedStmts.incrementAndGet();
								removedKvs.addAndGet(deletedKvs);
								if (_removedStmts % STATUS_UPDATE_INTERVAL == 0) {
									updateStatus(context);
								}
								return deletedKvs;
							}

							@Override
							protected void delete(KeyValue kv) throws IOException {
								rowKey.set(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
								try {
									context.write(rowKey, kv);
								} catch (InterruptedException ex) {
									throw new IOException(ex);
								}
							}

							@Override
							protected long getDefaultTimestamp(boolean delete) {
								return timestamp;
							}

							@Override
							public void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
								try (CloseableIteration<? extends Statement, SailException> iter = getStatements(subj, pred, obj, true, contexts)) {
									while (iter.hasNext()) {
										Statement st = iter.next();
										removeStatement(null, st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
									}
								}
							}
						};
					}
				});
				sail.setTrackResultSize(true);
                Function fn = new ParallelSplitFunction(qis.getRepeatIndex());
                sail.getFunctionRegistry().add(fn);
                try {
                    SailRepository rep = new SailRepository(sail);
                    try {
                        rep.init();
                        try(SailRepositoryConnection con = rep.getConnection()) {
	                        Update upd = new HBaseUpdate(singleUpdate, sail, con);
	                        Map<String,String> bindings = conf.getPropsWithPrefix(BINDING_PROPERTY_PREFIX);
	                        for (Map.Entry<String,String> binding : bindings.entrySet()) {
	                        	upd.setBinding(binding.getKey(), NTriplesUtil.parseValue(binding.getValue(), rep.getValueFactory()));
	                        }
	                        context.setStatus(queryName);
	                        LOG.info("Executing update:\n{}", query);
	                        upd.execute();
                        }
                    } finally {
                        rep.shutDown();
                    }
                } finally {
                	sail.getFunctionRegistry().remove(fn);
                }
                updateStatus(context);
                LOG.info("Query finished with {} statements ({} KeyValues) added and {} ({} KeyValues) removed", addedStmts.get(), addedKvs.get(), removedStmts.get(), removedKvs.get());
            }
        }

		private void updateStatus(Context context) {
			long _addedStmts = addedStmts.get();
			long _removedStmts = removedStmts.get();
			long _addedKvs = addedKvs.get();
			long _removedKvs = removedKvs.get();
			context.getCounter(Counters.ADDED_STATEMENTS).setValue(_addedStmts);
			context.getCounter(Counters.ADDED_KVS).setValue(_addedKvs);
			context.getCounter(Counters.REMOVED_STATEMENTS).setValue(_removedStmts);
			context.getCounter(Counters.REMOVED_KVS).setValue(_removedKvs);
            context.setStatus(String.format("%s - %d (%d) added %d (%d) removed", queryName, _addedStmts, _addedKvs, _removedStmts, _removedKvs));
            LOG.info("{} statements ({} KeyValues) added and {} ({} KeyValues) removed",     _addedStmts, _addedKvs, _removedStmts, _removedKvs);
		}
    }

    public HalyardBulkUpdate() {
        super(
            TOOL_NAME,
            "Halyard Bulk Update is a MapReduce application that executes multiple SPARQL Update operations in parallel in the Mapper phase. "
                + "The Shuffle and Reduce phase are responsible for the efficient update of the dataset in a bulk mode (similar to the Halyard Bulk Load). "
                + "Halyard Bulk Update supports large-scale DELETE/INSERT operations that are not executed separately, but instead they are processed as a single atomic bulk operation at the end of the execution.",
            "Example: halyard bulkupdate -s my_dataset -q hdfs:///myupdates/*.sparql -w hdfs:///my_tmp_workdir"
        );
        addOption("s", "source-dataset", "dataset_table", TABLE_NAME_PROPERTY, "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "update-operations", "sparql_update_operations", "folder or path pattern with SPARQL update operations (this or --update-operation is required)", false, true);
        addOption(null, "update-operation", "sparql_update_operation", "SPARQL update operation to be executed (this or -q is required)", false, true);
        addOption("w", "work-dir", "shared_folder", "Unique non-existent folder within shared filesystem to server as a working directory for the temporary HBase files,  the files are moved to their final HBase locations during the last stage of the load process", true, true);
        addOption("e", "target-timestamp", "timestamp", TIMESTAMP_PROPERTY, "Optionally specify timestamp of all updated records (default is actual time of the operation)", false, true);
        addOption("i", "elastic-index", "elastic_index_url", HBaseSail.ELASTIC_INDEX_URL, "Optional ElasticSearch index URL", false, true);
        addKeyValueOption("$", "binding=value", BINDING_PROPERTY_PREFIX, "Optionally specify bindings");
        addOption(null, "dry-run", null, DRY_RUN_PROPERTY, "Skip loading of HFiles", false, true);
    }


    public int run(CommandLine cmd) throws Exception {
        configureString(cmd, 's', null);
        String queryFiles = cmd.getOptionValue('q');
        String query = cmd.getOptionValue("update-operation");
        if (queryFiles == null && query == null) {
        	throw new MissingOptionException("One of -q or --update-operation is required");
        }
        String workdir = cmd.getOptionValue('w');
        configureString(cmd, 'i', null);
        configureBindings(cmd, '$');
        configureBoolean(cmd, "dry-run");
        configureLong(cmd, 'e', System.currentTimeMillis());
        return (run(getConf(), queryFiles, query, workdir) != null) ? 0 : -1;
    }

    static List<JsonInfo> executeUpdate(Configuration conf, String source, String query, Map<String,Value> bindings) throws Exception {
    	Configuration jobConf = new Configuration(conf);
    	jobConf.set(TABLE_NAME_PROPERTY, source);
    	for (Map.Entry<String,Value> binding : bindings.entrySet()) {
    		conf.set(BINDING_PROPERTY_PREFIX+binding.getKey(), NTriplesUtil.toNTriplesString(binding.getValue(), true));
    	}
    	String workdir = "work/" + TOOL_NAME + "-" + UUID.randomUUID();
    	try {
    		return run(jobConf, null, query, workdir);
    	} finally {
    		FileSystem.get(conf).delete(new Path(workdir), true);
    	}
    }

    private static List<JsonInfo> run(Configuration conf, String queryFiles, String query, String workdir) throws IOException, InterruptedException, ClassNotFoundException {
    	String source = conf.get(TABLE_NAME_PROPERTY);
        TableMapReduceUtil.addDependencyJarsForClasses(conf,
               NTriplesUtil.class,
               Rio.class,
               AbstractRDFHandler.class,
               RDFFormat.class,
               RDFParser.class,
               Table.class,
               HBaseConfiguration.class,
               AuthenticationProtos.class);
        HBaseConfiguration.addHbaseResources(conf);
        List<JsonInfo> infos = new ArrayList<>();
        int stages = 1;
        for (int stage = 0; stage < stages; stage++) {
            Job job = Job.getInstance(conf, "HalyardBulkUpdate -> " + workdir + " -> " + source + " stage #" + stage);
            job.getConfiguration().setInt(STAGE_PROPERTY, stage);
            job.setJarByClass(HalyardBulkUpdate.class);
            job.setMapperClass(SPARQLUpdateMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);
            job.setInputFormatClass(QueryInputFormat.class);
            job.setSpeculativeExecution(false);
			Connection conn = HalyardTableUtils.getConnection(conf);
			try (Table hTable = HalyardTableUtils.getTable(conn, source, false, 0)) {
				RegionLocator regionLocator = conn.getRegionLocator(hTable.getName());
				HFileOutputFormat2.configureIncrementalLoad(job, hTable.getDescriptor(), regionLocator);
				if (queryFiles != null) {
					QueryInputFormat.setQueriesFromDirRecursive(job.getConfiguration(), queryFiles, true, stage);
				} else {
		            QueryInputFormat.addQuery(job.getConfiguration(), "update-operation", query, true, stage);
				}
                Path outPath = new Path(workdir, "stage"+stage);
                FileOutputFormat.setOutputPath(job, outPath);
                TableMapReduceUtil.addDependencyJars(job);
                TableMapReduceUtil.initCredentials(job);
                if (stage == 0) { //count real number of stages
                    for (InputSplit is : new QueryInputFormat().getSplits(job)) {
                        QueryInputFormat.QueryInputSplit qis = (QueryInputFormat.QueryInputSplit)is;
                        int updates = QueryParserUtil.parseUpdate(QueryLanguage.SPARQL, qis.getQuery(), null).getUpdateExprs().size();
                        if (updates > stages) {
                            stages = updates;
                        }
                        LOG.info("{} contains {} stages of the update sequence.", qis.getQueryName(), updates);
                    }
                    LOG.info("Bulk Update will process {} MapReduce stages.", stages);
                }
                if (job.waitForCompletion(true)) {
                	infos.add(JsonInfo.from(job));
    				bulkLoad(conf, hTable.getName(), outPath);
                    LOG.info("Stage #{} of {} completed.", stage+1, stages);
                } else {
            		LOG.error("Stage #{} of {} failed to complete.", stage+1, stages);
                    return null;
                }
            }
        }
        LOG.info("Bulk Update completed.");
        return infos;
    }


    static final class JsonInfo extends HttpSparqlHandler.JsonUpdateInfo {
    	public String name;
    	public String id;
    	public String trackingURL;

    	static JsonInfo from(Job job) throws IOException {
    		JsonInfo info = new JsonInfo();
    		info.name = job.getJobName();
    		info.id = job.getJobID().getJtIdentifier();
    		info.trackingURL = job.getTrackingURL();
    		info.totalInserted = job.getCounters().findCounter(Counters.ADDED_STATEMENTS).getValue();
    		info.totalDeleted = job.getCounters().findCounter(Counters.ADDED_STATEMENTS).getValue();
    		return info;
    	}
    }
}
