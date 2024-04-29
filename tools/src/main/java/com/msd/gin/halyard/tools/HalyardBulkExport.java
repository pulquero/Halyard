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

import static com.msd.gin.halyard.model.vocabulary.HALYARD.*;

import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.repository.HBaseRepository;
import com.msd.gin.halyard.sail.ElasticSettings;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;
import com.msd.gin.halyard.sail.search.SearchDocument;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Hadoop MapReduce tool for batch exporting of SPARQL queries.
 * @author Adam Sotona (MSD)
 */
public final class HalyardBulkExport extends AbstractHalyardTool {
    private static final Logger LOG = LoggerFactory.getLogger(HalyardBulkExport.class);

    private static final String SOURCE_TABLE = "halyard.bulkexport.source";
    private static final String TARGET_URL = "halyard.bulkexport.target";
    private static final String JDBC_DRIVER = "halyard.bulkexport.jdbc.driver";
    private static final String JDBC_CLASSPATH = "halyard.bulkexport.jdbc.classpath";
    private static final String JDBC_PROPERTIES = "halyard.bulkexport.jdbc.properties";

    enum Counters {
		EXPORTED_STATEMENTS
	}

    /**
     * Mapper class performing SPARQL Graph query evaluation and producing Halyard KeyValue pairs for HBase BulkLoad Reducers
     */
    public static final class BulkExportMapper extends Mapper<NullWritable, Void, NullWritable, Object> {

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            final QueryInputFormat.QueryInputSplit qis = (QueryInputFormat.QueryInputSplit)context.getInputSplit();
            final String query = qis.getQuery();
            final String name = qis.getQueryName();
            final int partitionIndex = qis.getRepeatIndex();
            int dot = name.indexOf('.');
            final String bName = dot > 0 ? name.substring(0, dot) : name;
            context.setStatus("Execution of: " + name);
            LOG.info("Executing query {}:\n{}", name, query);
            HalyardExport.StatusLog log = new HalyardExport.StatusLog() {
                @Override
                public void tick() {
                    context.progress();
                	context.getCounter(Counters.EXPORTED_STATEMENTS).increment(1L);
                }
                @Override
                public void logStatus(String status) {
                    context.setStatus(name + ": " + status);
                }
            };
            Configuration cfg = context.getConfiguration();
            String[] props = cfg.getStrings(JDBC_PROPERTIES);
            if (props != null) {
                for (int i=0; i<props.length; i++) {
                    props[i] = new String(Base64.decodeBase64(props[i]), StandardCharsets.UTF_8);
                }
            }
            String source = cfg.get(SOURCE_TABLE);
        	HBaseSail sail = new HBaseSail(cfg, source, false, 0, true, 0, ElasticSettings.from(cfg), null);
        	HBaseRepository repo = new HBaseRepository(sail);
            try {
            	repo.init();
            	QueryBindingSet allBindings = new QueryBindingSet();
				allBindings.setBinding(HBaseSailConnection.FORK_INDEX_BINDING, repo.getValueFactory().createLiteral(partitionIndex));
                BindingSet configBindings = AbstractHalyardTool.getBindings(cfg, repo.getValueFactory());
                for (Binding binding : configBindings) {
                	allBindings.setBinding(binding.getName(), binding.getValue());
                }

                HalyardExport.QueryResultWriter writer;
                if (cfg.get("es.resource") != null) {
                	writer = new EsOutputResultWriter(log, sail.getRDFFactory(), sail.getValueFactory(), context);
                } else {
                    String target = cfg.get(TARGET_URL);
                	writer = HalyardExport.createWriter(sail.getConfiguration(), log, MessageFormat.format(target, bName, partitionIndex), sail.getRDFFactory(), sail.getValueFactory(), cfg.get(JDBC_DRIVER), cfg.get(JDBC_CLASSPATH), props, false);
                }
                try {
	        		HalyardExport.export(repo, query, allBindings, writer);
                } catch (ExportInterruptedException e) {
                	throw (InterruptedException) e.getCause();
	            } finally {
	        		writer.close();
	            }
            } finally {
            	repo.shutDown();
            }
        }

        private static class EsOutputResultWriter extends HalyardExport.QueryResultWriter {
            private final Text outputJson = new Text();
        	private final RDFFactory rdfFactory;
        	private final ValueFactory valueFactory;
        	private final Context output;

        	EsOutputResultWriter(HalyardExport.StatusLog log, RDFFactory rdfFactory, ValueFactory valueFactory, Context output) {
    			super(log);
    			this.rdfFactory = rdfFactory;
    			this.valueFactory = valueFactory;
    			this.output = output;
    		}

    		@Override
    		public void writeTupleQueryResult(TupleQueryResult queryResult) throws IOException {
        		List<String> bindingNames = queryResult.getBindingNames();
        		List<String> auxBindingNames = new ArrayList<>(bindingNames.size());
        		for (String bindingName : bindingNames) {
        			if (!"value".equals(bindingName)) {
        				auxBindingNames.add(bindingName);
        			}
        		}

        		for (BindingSet bs : queryResult) {
        			HalyardElasticIndexer.JsonDocumentWriter writer = new HalyardElasticIndexer.JsonDocumentWriter(rdfFactory, valueFactory);
	    			Value value = bs.getValue("value");
	    			String id = writer.writeValue(value);
	    			if (id != null) {
		    			for (String binding : auxBindingNames) {
	        				Value v = bs.getValue(binding);
	        				if (v != null) {
	        					writer.writeObjectField(binding, v);
	        				}
	        			}
		    			writer.close();
		    			outputJson.set(writer.toString());
						try {
							output.write(NullWritable.get(), outputJson);
						} catch (InterruptedException e) {
							throw new ExportInterruptedException(e);
						}
	            		tick();
	    			}
            	}
    		}

    		@Override
    		public void writeGraphQueryResult(GraphQueryResult queryResult) throws IOException {
                throw new HalyardExport.ExportException("Elasticsearch does not support graph query results.");
    		}

    		@Override
    		protected void doClose() {
    			// do nothing
    		}
        }
    }

    private static class ExportInterruptedException extends HalyardExport.ExportException {
		private static final long serialVersionUID = -6625839941463116026L;

		ExportInterruptedException(InterruptedException cause) {
			super(cause);
		}
    }

    public HalyardBulkExport() {
        super("bulkexport",
            "Halyard Bulk Export is a MapReduce application that executes multiple Halyard Exports in MapReduce framework. "
                + "Query file name (without extension) can be used in the target URL pattern. Order of queries execution is not guaranteed. "
                + "Another internal level of parallelisation is done using a custom SPARQL function halyard:" + PARALLEL_SPLIT_FUNCTION.stringValue() + "(<constant_number_of_forks>, ?a_binding, ...). "
                + "The function takes one or more bindings as its arguments and these bindings are used as keys to randomly distribute the query evaluation across the executed parallel forks of the same query.",
            "Example: halyard bulkexport -s my_dataset -q hdfs:///myqueries/*.sparql -t hdfs:/my_folder/{0}-{1}.csv.gz"
        );
        addOption("s", "source-dataset", "dataset_table", SOURCE_TABLE, "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "queries", "sparql_queries", "folder or path pattern with SPARQL tuple or graph queries (this or --query is required)", false, true);
        addOption(null, "query", "sparql_query", "SPARQL tuple or graph query (this or -q is required)", false, true);
        addOption("t", "target-url", "target_url", TARGET_URL, "file://<path>/{0}-{1}.<ext> or hdfs://<path>/{0}-{1}.<ext> or jdbc:<jdbc_connection>/{0}, where {0} is replaced query filename (without extension) and {1} is replaced with parallel fork index (when " + PARALLEL_SPLIT_FUNCTION.stringValue() + " function is used in the particular query)", true, true);
        addOption("p", "jdbc-property", "property=value", JDBC_PROPERTIES, "JDBC connection property", false, false);
        addOption("l", "jdbc-driver-classpath", "driver_classpath", JDBC_CLASSPATH, "JDBC driver classpath delimited by ':'", false, true);
        addOption("c", "jdbc-driver-class", "driver_class", JDBC_DRIVER, "JDBC driver class name", false, true);
        addOption("i", "elastic-index", "elastic_index_url", ElasticSettings.ELASTIC_INDEX_URL, "Optional ElasticSearch index URL", false, true);
        addKeyValueOption("$", null, "binding=value", BINDING_PROPERTY_PREFIX, "Optionally specify bindings");
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
        if (!cmd.getArgList().isEmpty()) {
        	throw new HalyardExport.ExportException("Unknown arguments: " + cmd.getArgList().toString());
        }
        configureString(cmd, 's', null);
        String queryFiles = cmd.getOptionValue('q');
        String query = cmd.getOptionValue("query");
        if (queryFiles == null && query == null) {
        	throw new MissingOptionException("One of -q or --query is required");
        }
        configureString(cmd, 't', null);
        String target = getConf().get(TARGET_URL);
        boolean isEsExport = HalyardExport.isElasticsearch(target);
        if (!isEsExport) {
	        if (queryFiles != null && !target.contains("{0}")) {
	            throw new HalyardExport.ExportException("Bulk export target must contain '{0}' to be replaced by stripped filename of the actual SPARQL query.");
	        } else if (query != null && target.contains("{0}")) {
	            throw new HalyardExport.ExportException("Bulk export target cannot contain '{0}' when using --query.");
	        }
        }
        configureString(cmd, 'c', null);

        String cp = cmd.getOptionValue('l');
        if (cp != null) {
            String jars[] = cp.split(File.pathSeparator);
            StringBuilder newCp = new StringBuilder();
            String pathSep = "";
            for (String jar : jars) {
                newCp.append(pathSep).append(addTmpFile(getConf(), jar)); //append classpath entries to tmpfiles and trim paths from the classpath
                pathSep = File.pathSeparator;
            }
            getConf().set(JDBC_CLASSPATH, newCp.toString());
        }

        String props[] = cmd.getOptionValues('p');
        if (props != null) {
            for (int i=0; i<props.length; i++) {
                props[i] = Base64.encodeBase64String(props[i].getBytes(StandardCharsets.UTF_8));
            }
            getConf().setStrings(JDBC_PROPERTIES, props);
        }

        configureString(cmd, 'i', null);
        configureBindings(cmd, '$');

        return (run(getConf(), queryFiles, query) != null) ? 0 : -1;
    }

    static List<JsonInfo> executeExport(Configuration conf, String source, String query, String target, Map<String,Value> bindings) throws Exception {
    	Configuration jobConf = new Configuration(conf);
    	jobConf.set(SOURCE_TABLE, source);
    	jobConf.set(TARGET_URL, target);
    	for (Map.Entry<String,Value> binding : bindings.entrySet()) {
    		jobConf.set(BINDING_PROPERTY_PREFIX+binding.getKey(), NTriplesUtil.toNTriplesString(binding.getValue(), true));
    	}
    	return run(jobConf, null, query);
    }

    private static List<JsonInfo> run(Configuration conf, String queryFiles, String query) throws IOException, InterruptedException, ClassNotFoundException {
    	String source = conf.get(SOURCE_TABLE);
    	String target = conf.get(TARGET_URL);
    	boolean isEsExport = HalyardExport.isElasticsearch(target);
        if (isEsExport) {
        	URL targetUrl = new URL(target);
            String indexName = targetUrl.getPath().substring(1);
            conf.set("es.nodes", targetUrl.getHost()+":"+targetUrl.getPort());
            conf.set("es.resource", indexName);
            conf.set("es.mapping.id", SearchDocument.ID_FIELD);
            conf.set("es.input.json", "yes");
        }

        TableMapReduceUtil.addDependencyJarsForClasses(conf,
               NTriplesUtil.class,
               Rio.class,
               AbstractRDFHandler.class,
               RDFFormat.class,
               RDFParser.class,
               Table.class,
               HBaseConfiguration.class,
               AuthenticationProtos.class);
        if (System.getProperty("exclude.es-hadoop") == null) {
         	TableMapReduceUtil.addDependencyJarsForClasses(conf, EsOutputFormat.class);
        }
        HBaseConfiguration.addHbaseResources(conf);
        BindingSet bindings = AbstractHalyardTool.getBindings(conf, SimpleValueFactory.getInstance());
        Job job = Job.getInstance(conf, "HalyardBulkExport " + source + " -> " + target);
        job.setJarByClass(HalyardBulkExport.class);
        job.setMaxMapAttempts(1);
        job.setMapperClass(BulkExportMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.setSpeculativeExecution(false);
        job.setInputFormatClass(QueryInputFormat.class);
		if (queryFiles != null) {
			QueryInputFormat.setQueriesFromDirRecursive(job.getConfiguration(), queryFiles, -1, bindings);
		} else {
            QueryInputFormat.addQuery(job.getConfiguration(), "query", query, -1, bindings);
		}
        if (isEsExport) {
        	job.setOutputFormatClass(EsOutputFormat.class);
            job.setMapOutputValueClass(Text.class);
        } else {
        	job.setOutputFormatClass(NullOutputFormat.class);
            job.setMapOutputValueClass(Void.class);
        }
        TableMapReduceUtil.initCredentials(job);
        if (job.waitForCompletion(true)) {
            LOG.info("Bulk Export completed.");
            return Collections.singletonList(JsonInfo.from(job));
        } else {
    		LOG.error("Bulk Export failed to complete.");
            return null;
        }
    }

    private static String addTmpFile(Configuration conf, String file) throws IOException {
        Path path = new Path(new File(file).toURI());
        // org.apache.hadoop.mapreduce.JobResourceUploader tmpfiles
        String tmpFiles = conf.get("tmpfiles");
        conf.set("tmpfiles", tmpFiles == null ? path.toString() : tmpFiles + "," + path.toString());
        return path.getName();
    }


    static final class JsonInfo extends HttpSparqlHandler.JsonExportInfo {
    	public String name;
    	public String id;
    	public String trackingURL;

    	static JsonInfo from(Job job) throws IOException {
    		JsonInfo info = new JsonInfo();
    		info.name = job.getJobName();
    		info.id = job.getJobID().getJtIdentifier();
    		info.trackingURL = job.getTrackingURL();
    		info.totalExported = job.getCounters().findCounter(Counters.EXPORTED_STATEMENTS).getValue();
    		return info;
    	}
    }
}
