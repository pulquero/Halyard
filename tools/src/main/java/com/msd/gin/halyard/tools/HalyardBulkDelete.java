/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardBulkDelete extends AbstractHalyardTool {
    private static final Logger LOG = LoggerFactory.getLogger(HalyardBulkDelete.class);

    static final String DEFAULT_GRAPH_KEYWORD = "DEFAULT";
	private static final String TOOL_NAME = "bulkdelete";
    private static final String SOURCE = confProperty(TOOL_NAME, "source");
    private static final String SNAPSHOT_PATH = confProperty(TOOL_NAME, "snapshot");
    private static final String SUBJECT_PROPERTY = confProperty(TOOL_NAME, "subject");
    private static final String PREDICATE_PROPERTY = confProperty(TOOL_NAME, "predicate");
    private static final String OBJECT_PROPERTY = confProperty(TOOL_NAME, "object");
    private static final String CONTEXTS = confProperty(TOOL_NAME, "contexts");
    private static final long STATUS_UPDATE_INTERVAL = 100000L;

    enum Counters {
		REMOVED_KVS,
		REMOVED_TRIPLED_KVS,
		TOTAL_KVS
	}

    static final class DeleteMapper extends RdfTableMapper<ImmutableBytesWritable, KeyValue> {

        final ImmutableBytesWritable outRowKey = new ImmutableBytesWritable();
        long totalKvs = 0L, deletedKvs = 0L, deletedTripledKvs = 0L;
        long htimestamp;
        boolean tripleCleanupOnly;
        Resource subj;
        IRI pred;
        Value obj;
        Set<IRI> ctxs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            openKeyspace(conf, conf.get(SOURCE), conf.get(SNAPSHOT_PATH));
            htimestamp = HalyardTableUtils.toHalyardTimestamp(conf.getLong(TIMESTAMP_PROPERTY, System.currentTimeMillis()), false);
            String s = conf.get(SUBJECT_PROPERTY);
            if (s != null) {
                subj = NTriplesUtil.parseURI(s, vf);
            }
            String p = conf.get(PREDICATE_PROPERTY);
            if (p != null) {
                pred = NTriplesUtil.parseURI(p, vf);
            }
            String o = conf.get(OBJECT_PROPERTY);
            if (o != null) {
                obj = NTriplesUtil.parseValue(o, vf);
            }
            String contextList = conf.get(CONTEXTS);
            if (contextList != null) {
                ctxs = new HashSet<>();
                for (String c : contextList.split(" +")) {
                    if (DEFAULT_GRAPH_KEYWORD.equals(c)) {
                        ctxs.add(null);
                    } else {
                        ctxs.add(NTriplesUtil.parseURI(c, vf));
                    }
                }
            }
        }

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result value, Context output) throws IOException, InterruptedException {
            StatementIndex<?,?,?,?> index = stmtIndices.toIndex(rowKey.get()[rowKey.getOffset()]);
            for (Cell c : value.rawCells()) {
                Statement st = stmtIndices.parseStatement(null, null, null, null, c, vf);
                if (HALYARD.TRIPLE_GRAPH_CONTEXT.equals(st.getContext())) {
                    cleanupTriple(c, st, output);
                } else if ((ctxs == null || ctxs.contains(st.getContext())) && (subj == null || subj.equals(st.getSubject())) && (pred == null || pred.equals(st.getPredicate())) && (obj == null || obj.equals(st.getObject()))) {
                    deleteCell(c, st, output);
                } else {
                    output.progress();
                }
                if (totalKvs++ % STATUS_UPDATE_INTERVAL == 0) {
                    String msg = MessageFormat.format("{0}: {1} / {2} cells deleted", index, deletedKvs, totalKvs);
                    output.setStatus(msg);
                    LOG.info(msg);
                }
            }
        }

        private void deleteCell(Cell c, Statement st, Context output) throws IOException, InterruptedException {
        	KeyValue kv = new KeyValue(c.getRowArray(), c.getRowOffset(), (int) c.getRowLength(),
                c.getFamilyArray(), c.getFamilyOffset(), (int) c.getFamilyLength(),
                c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength(),
                htimestamp, KeyValue.Type.DeleteColumn, c.getValueArray(), c.getValueOffset(),
                c.getValueLength());
            outRowKey.set(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
            output.write(outRowKey, kv);
            deletedKvs++;
            if (st.getSubject().isTriple() || st.getObject().isTriple()) {
                deletedTripledKvs++;
            }
        }

        private void cleanupTriple(Cell c, Statement st, Context output) throws IOException, InterruptedException {
        	Triple t = vf.createTriple(st.getSubject(), st.getPredicate(), st.getObject());
    		if (!stmtIndices.isTripleReferenced(keyspaceConn, t)) {
    			// orphaned so safe to remove
    			deleteCell(c, st, output);
    		}
        }

        @Override
        protected void cleanup(Context output) throws IOException {
        	output.getCounter(Counters.REMOVED_KVS).increment(deletedKvs);
        	output.getCounter(Counters.REMOVED_TRIPLED_KVS).increment(deletedTripledKvs);
        	output.getCounter(Counters.TOTAL_KVS).increment(totalKvs);
        	closeKeyspace();
        }
    }

    public HalyardBulkDelete() {
        super(
            TOOL_NAME,
            "Halyard Bulk Delete is a MapReduce application that effectively deletes large set of triples or whole named graphs, based on specified statement pattern and/or named graph(s).",
            "Example: halyard bulkdelete -t my_data -w bulkdelete_temp1 -s <http://whatever/mysubj> -g <http://whatever/mygraph1> -g <http://whatever/mygraph2>"
        );
        addOption("t", "target-dataset", "dataset_table", "HBase table with Halyard RDF store to update", true, true);
        addOption("w", "work-dir", "shared_folder", "Temporary folder for HBase files", true, true);
        addOption("s", "subject", "subject", SUBJECT_PROPERTY, "Optional subject to delete", false, true);
        addOption("p", "predicate", "predicate", PREDICATE_PROPERTY, "Optional predicate to delete", false, true);
        addOption("o", "object", "object", OBJECT_PROPERTY, "Optional object to delete", false, true);
        addOption("g", "named-graph", "named_graph", "Optional named graph(s) to delete, "+DEFAULT_GRAPH_KEYWORD+" represents triples outside of any named graph", false, false);
        addOption("e", "target-timestamp", "timestamp", "Optionally specify timestamp of all deleted records (default is actual time of the operation)", false, true);
        addOption("n", "snapshot-name", "snapshot_name", "Snapshot to read from. If specified then data is read from the snapshot instead of the table specified by -t and the results are written to the table. Requires -u.", false, true);
        addOption("u", "restore-dir", "restore_folder", "The snapshot restore folder on HDFS. Requires -n.", false, true);
        addOption(null, "dry-run", null, DRY_RUN_PROPERTY, "Skip loading of HFiles", false, true);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
    	if ((cmd.hasOption('n') && !cmd.hasOption('u')) || (!cmd.hasOption('n') && cmd.hasOption('u'))) {
    		throw new MissingOptionException("Both -n and -u must be specified to read from a snapshot");
    	}
        String target = cmd.getOptionValue('t');
    	boolean useSnapshot = cmd.hasOption('n') && cmd.hasOption('u');
        String source = useSnapshot ? cmd.getOptionValue('n') : target;
        getConf().set(SOURCE, source);
        getConf().setLong(TIMESTAMP_PROPERTY, cmd.hasOption('e') ? Long.parseLong(cmd.getOptionValue('e')) : System.currentTimeMillis());
        if (cmd.hasOption('u')) {
			FileSystem fs = CommonFSUtils.getRootDirFileSystem(getConf());
        	if (fs.exists(new Path(cmd.getOptionValue('u')))) {
        		throw new IOException("Snapshot restore directory already exists");
        	}
        	getConf().set(SNAPSHOT_PATH, cmd.getOptionValue('u'));
        }
        configureString(cmd, 's', null, this::validateSubject);
        configureString(cmd, 'p', null, this::validatePredicate);
        configureString(cmd, 'o', null, this::validateObject);
        String[] ntGraphs = cmd.getOptionValues('g');  // N-triples encoded
        if (ntGraphs != null) {
        	// URL-safe separated list
        	getConf().set(CONTEXTS, String.join(" ", validateContexts(ntGraphs)));
        }
        configureBoolean(cmd, "dry-run");
        String snapshotPath = getConf().get(SNAPSHOT_PATH);

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

        RDFFactory rdfFactory;
        Keyspace keyspace = getKeyspace(source, snapshotPath);
        try {
       		rdfFactory = loadRDFFactory(keyspace);
		} finally {
			keyspace.close();
		}
        ValueFactory vf = new IdValueFactory(rdfFactory);
        StatementIndices indices = new StatementIndices(getConf(), rdfFactory);
        List<Scan> scans;
        if (ntGraphs != null && ntGraphs.length > 0) {
        	scans = new ArrayList<>(1 + ntGraphs.length);
        	scans.add(indices.scanDefaultIndices());
        	for (String ntGraph : ntGraphs) {
        		if (!DEFAULT_GRAPH_KEYWORD.equals(ntGraph)) {
        			Resource ctx = NTriplesUtil.parseURI(ntGraph, vf);
            		scans.addAll(indices.scanContextIndices(ctx));
        		}
        	}
        } else {
        	scans = Collections.singletonList(indices.scanAll());
        }
        try {
            for (int i=0; !scans.isEmpty(); i++) {
                Job job = Job.getInstance(getConf(), "HalyardDelete " + source);
                job.setJarByClass(HalyardBulkDelete.class);
                keyspace.initMapperJob(
                    scans,
                    DeleteMapper.class,
                    ImmutableBytesWritable.class,
                    LongWritable.class,
                    job);
                job.setMapOutputKeyClass(ImmutableBytesWritable.class);
                job.setMapOutputValueClass(KeyValue.class);
                job.setSpeculativeExecution(false);
                TableName hTableName;
        		try (Connection conn = HalyardTableUtils.getConnection(getConf())) {
        			try (Table hTable = HalyardTableUtils.getTable(conn, target, false, 0)) {
        				hTableName = hTable.getName();
        				RegionLocator regionLocator = conn.getRegionLocator(hTableName);
        				HFileOutputFormat2.configureIncrementalLoad(job, hTable.getDescriptor(), regionLocator);
        			}
        		}
        		Path workDir = new Path(cmd.getOptionValue('w'), "pass"+(i+1));
                FileOutputFormat.setOutputPath(job, workDir);
	            if (job.waitForCompletion(true)) {
					bulkLoad(job, hTableName, workDir);
	                LOG.info("Bulk Delete completed.");
	                if (!isDryRun(getConf()) && job.getCounters().findCounter(Counters.REMOVED_TRIPLED_KVS).getValue() > 0) {
	                	// maybe more triples to delete
	                	LOG.info("Removing any orphaned triples...");
		                RDFContext rdfGraphCtx = rdfFactory.createContext(HALYARD.TRIPLE_GRAPH_CONTEXT);
		                scans = Arrays.asList(
		    	            indices.getCSPOIndex().scan(rdfGraphCtx),
		    	            indices.getCPOSIndex().scan(rdfGraphCtx),
		    	            indices.getCOSPIndex().scan(rdfGraphCtx)
		                );
		                if (useSnapshot) {
		                	// switch to reading from table
		                	getConf().set(SOURCE, target);
		                	getConf().unset(SNAPSHOT_PATH);
		                	keyspace.close();
		                	keyspace.destroy();
		                	keyspace = getKeyspace(target, null);
		                }
	                } else {
	                	scans = Collections.emptyList();
	                }
	            } else {
	        		LOG.error("Bulk Delete failed to complete.");
	                return -1;
	            }
            }
        } finally {
        	keyspace.destroy();
        }
        LOG.info("Bulk Delete completed.");
        return 0;
    }

    private String validateIRI(String s) {
		NTriplesUtil.parseURI(s, SimpleValueFactory.getInstance());
		return s;
    }

    private String validateSubject(String s) {
    	return validateIRI(s);
    }

    private String validatePredicate(String s) {
    	return validateIRI(s);
    }

    private String validateObject(String s) {
		NTriplesUtil.parseValue(s, SimpleValueFactory.getInstance());
		return s;
    }

    private String[] validateContexts(String... s) {
    	for (String c : s) {
    		if (!DEFAULT_GRAPH_KEYWORD.equals(c)) {
    			validateIRI(c);
    		}
    	}
    	return s;
    }
}
