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
import com.msd.gin.halyard.repository.HBaseUpdate;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
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
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

/**
 * Apache Hadoop MapReduce tool for performing SPARQL Graph construct queries and then bulk loading the results back into HBase. Essentially, batch process queries
 * and bulk load the results.
 * @author Adam Sotona (MSD)
 */
public final class HalyardBulkUpdate extends AbstractHalyardTool {

    /**
     * String name of a custom SPARQL function to decimate parallel evaluation based on Mapper index
     */
    public static final String DECIMATE_FUNCTION_NAME = "decimateBy";

    /**
     * Property defining optional ElasticSearch index URL
     */
    public static final String ELASTIC_INDEX_URL = "halyard.elastic.index.url";

    static final String TIMESTAMP_BINDING_NAME = "HALYARD_TIMESTAMP_SPECIAL_VARIABLE";
    static final String TIMESTAMP_CALLBACK_BINDING_NAME = "HALYARD_TIMESTAMP_SPECIAL_CALLBACK_BINDING";

    /**
     * Full URI of a custom SPARQL function to decimate parallel evaluation based on Mapper index
     */
    public static final String DECIMATE_FUNCTION_URI = HALYARD.NAMESPACE + DECIMATE_FUNCTION_NAME;
    private static final String TABLE_NAME_PROPERTY = "halyard.table.name";
    private static final String STAGE_PROPERTY = "halyard.update.stage";

    /**
     * Mapper class performing SPARQL Graph query evaluation and producing Halyard KeyValue pairs for HBase BulkLoad Reducers
     */
    public static final class SPARQLUpdateMapper extends Mapper<NullWritable, Void, ImmutableBytesWritable, KeyValue> {

        private String tableName;
        private String elasticIndexURL;
        private long defaultTimestamp;
        private int stage;

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            tableName = conf.get(TABLE_NAME_PROPERTY);
            elasticIndexURL = conf.get(ELASTIC_INDEX_URL);
            defaultTimestamp = conf.getLong(DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis());
            stage = conf.getInt(STAGE_PROPERTY, 0);
            final QueryInputFormat.QueryInputSplit qis = (QueryInputFormat.QueryInputSplit)context.getInputSplit();
            final String query = qis.getQuery();
            final String name = qis.getQueryName();
            ParsedUpdate parsedUpdate = QueryParserUtil.parseUpdate(QueryLanguage.SPARQL, query, null);
            if (parsedUpdate.getUpdateExprs().size() <= stage) {
                context.setStatus("Nothing to execute in: " + name + " for stage #" + stage);
            } else {
                UpdateExpr ue = parsedUpdate.getUpdateExprs().get(stage);
                LOG.info(ue.toString());
                ParsedUpdate singleUpdate = new ParsedUpdate(parsedUpdate.getSourceString(), parsedUpdate.getNamespaces());
                singleUpdate.addUpdateExpr(ue);
                Dataset d = parsedUpdate.getDatasetMapping().get(ue);
                if (d != null) {
                    singleUpdate.map(ue, d);
                }
                context.setStatus("Execution of: " + name + " stage #" + stage);
                final AtomicLong added = new AtomicLong();
                final AtomicLong removed = new AtomicLong();
                Function fn = new ParallelSplitFunction(qis.getRepeatIndex());
                FunctionRegistry.getInstance().add(fn);
                try {
					final HBaseSail sail = new HBaseSail(context.getConfiguration(), tableName, false, 0, true, 0, elasticIndexURL, new HBaseSail.Ticker() {
                        @Override
                        public void tick() {
                            context.progress();
                        }
					}, new HBaseSail.ConnectionFactory() {
						@Override
						public SailConnection createConnection(HBaseSail sail) {
							return new HBaseSailConnection(sail) {
								@Override
								protected void put(KeyValue kv) throws IOException {
									try {
										context.write(new ImmutableBytesWritable(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()), kv);
									} catch (InterruptedException ex) {
										throw new IOException(ex);
									}
									if (added.incrementAndGet() % 1000l == 0) {
										context.setStatus(name + " - " + added.get() + " added " + removed.get() + " removed");
										LOG.info("{} KeyValues added and {} removed", added.get(), removed.get());
									}
								}

								@Override
								protected void delete(KeyValue kv) throws IOException {
								    kv = new KeyValue(kv.getRowArray(), kv.getRowOffset(), (int) kv.getRowLength(),
								            kv.getFamilyArray(), kv.getFamilyOffset(), (int) kv.getFamilyLength(),
								            kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
								            kv.getTimestamp(), KeyValue.Type.DeleteColumn, kv.getValueArray(), kv.getValueOffset(),
								            kv.getValueLength());
									try {
										context.write(new ImmutableBytesWritable(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()), kv);
									} catch (InterruptedException ex) {
										throw new IOException(ex);
									}
									if (removed.incrementAndGet() % 1000l == 0) {
										context.setStatus(name + " - " + added.get() + " added " + removed.get() + " removed");
										LOG.info("{} KeyValues added and {} removed", added.get(), removed.get());
									}
								}

								@Override
								protected long getDefaultTimestamp(boolean delete) {
									return defaultTimestamp;
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
                    SailRepository rep = new SailRepository(sail);
                    try {
                        rep.init();
                        try(SailRepositoryConnection con = rep.getConnection()) {
	                        Update upd = new HBaseUpdate(singleUpdate, sail, con);
	                        LOG.info("Execution of: {}", query);
	                        context.setStatus(name);
	                        upd.execute();
	                        context.setStatus(name + " - " + added.get() + " added " + removed.get() + " removed");
	                        LOG.info("Query finished with {} KeyValues added and {} removed", added.get(), removed.get());
                        }
                    } finally {
                        rep.shutDown();
                    }
                } catch (RepositoryException | MalformedQueryException | QueryEvaluationException | RDFHandlerException ex) {
                    LOG.error("Error running update", ex);
                    throw new IOException(ex);
                } finally {
                    FunctionRegistry.getInstance().remove(fn);
                }
            }
        }
    }
   public HalyardBulkUpdate() {
        super(
            "bulkupdate",
            "Halyard Bulk Update is a MapReduce application that executes multiple SPARQL Update operations in parallel in the Mapper phase. "
                + "The Shuffle and Reduce phase are responsible for the efficient update of the dataset in a bulk mode (similar to the Halyard Bulk Load). "
                + "Halyard Bulk Update supports large-scale DELETE/INSERT operations that are not executed separately, but instead they are processed as a single atomic bulk operation at the end of the execution.",
            "Example: halyard bulkupdate -s my_dataset -q hdfs:///myupdates/*.sparql -w hdfs:///my_tmp_workdir"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "update-operations", "sparql_update_operations", "folder or path pattern with SPARQL update operations", true, true);
        addOption("w", "work-dir", "shared_folder", "Unique non-existent folder within shared filesystem to server as a working directory for the temporary HBase files,  the files are moved to their final HBase locations during the last stage of the load process", true, true);
        addOption("e", "target-timestamp", "timestamp", "Optionally specify timestamp of all loaded records (default is actual time of the operation)", false, true);
        addOption("i", "elastic-index", "elastic_index_url", "Optional ElasticSearch index URL", false, true);
    }


    public int run(CommandLine cmd) throws Exception {
        String source = cmd.getOptionValue('s');
        String queryFiles = cmd.getOptionValue('q');
        String workdir = cmd.getOptionValue('w');
        getConf().setLong(DEFAULT_TIMESTAMP_PROPERTY, Long.parseLong(cmd.getOptionValue('e', String.valueOf(System.currentTimeMillis()))));
        if (cmd.hasOption('i')) getConf().set(ELASTIC_INDEX_URL, cmd.getOptionValue('i'));
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
        getConf().setStrings(TABLE_NAME_PROPERTY, source);
        getConf().setLong(DEFAULT_TIMESTAMP_PROPERTY, getConf().getLong(DEFAULT_TIMESTAMP_PROPERTY, System.currentTimeMillis()));
        int stages = 1;
        for (int stage = 0; stage < stages; stage++) {
            Job job = Job.getInstance(getConf(), "HalyardBulkUpdate -> " + workdir + " -> " + source + " stage #" + stage);
            job.getConfiguration().setInt(STAGE_PROPERTY, stage);
            job.setJarByClass(HalyardBulkUpdate.class);
            job.setMapperClass(SPARQLUpdateMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);
            job.setInputFormatClass(QueryInputFormat.class);
            job.setSpeculativeExecution(false);
            job.setReduceSpeculativeExecution(false);
			Connection conn = HalyardTableUtils.getConnection(getConf());
			try (Table hTable = HalyardTableUtils.getTable(conn, source, false, 0)) {
				RegionLocator regionLocator = conn.getRegionLocator(hTable.getName());
				HFileOutputFormat2.configureIncrementalLoad(job, hTable.getDescriptor(), regionLocator);
                QueryInputFormat.setQueriesFromDirRecursive(job.getConfiguration(), queryFiles, true, stage);
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
    				BulkLoadHFiles.create(getConf()).bulkLoad(hTable.getName(), outPath);
                    LOG.info("Stage #{} of {} completed.", stage, stages);
                } else {
            		LOG.error("Stage #{} of {} failed to complete.");
                    return -1;
                }
            }
        }
        LOG.info("Bulk Update completed.");
        return 0;
    }
}
