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

import com.google.common.collect.Iterables;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.repository.HBaseRepository;
import com.msd.gin.halyard.sail.ElasticSettings;
import com.msd.gin.halyard.sail.HBaseSail;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.GeneralSecurityException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.rio.Rio;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.transport.ElasticsearchTransport;

/**
 * Command line tool to run SPARQL queries and export the results into various target systems. This class could be extended or modified to add new types of
 * export targets.
 * @author Adam Sotona (MSD)
 */
public final class HalyardExport extends AbstractHalyardTool {

    /**
     * A generic exception during export
     */
    public static class ExportException extends IOException {
        private static final long serialVersionUID = 2946182537302463011L;

        /**
         * ExportException constructor
         * @param message String exception message
         */
        public ExportException(String message) {
            super(message);
        }

        /**
         * ExportException constructor
         * @param cause Throwable exception cause
         */
        public ExportException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * StatusLog is a simple service interface that is notified when some data are processed or status is changed.
     * It's purpose is to notify caller (for example MapReduce task) that the execution is still alive and about update of the status.
     */
    public interface StatusLog {

        /**
         * This method is called to notify that the process is still alive
         */
        public void tick();

        /**
         * This method is called whenever the status has changed
         * @param status String new status
         */
        public void logStatus(String status);
    }

    static abstract class QueryResultWriter implements AutoCloseable {
        private final AtomicLong counter = new AtomicLong();
        private final StatusLog log;
        private long startTime;

        QueryResultWriter(StatusLog log) {
            this.log = log;
        }

        public final long getExportedCount() {
        	return counter.get();
        }

        public final void initTimer() {
            startTime = System.currentTimeMillis();
        }

        protected final long tick() {
            log.tick();
            long count = counter.incrementAndGet();
            if ((count % 10000l) == 0) {
                long time = System.currentTimeMillis();
                log.logStatus(MessageFormat.format("Exported {0} records/triples in average speed {1}/s", count, countPerSecond(count, time - startTime)));
            }
            return count;
        }

        public abstract void writeTupleQueryResult(TupleQueryResult queryResult) throws IOException;
        public abstract void writeGraphQueryResult(GraphQueryResult queryResult) throws IOException;
        @Override
        public final void close() throws ExportException {
            long time = System.currentTimeMillis()+1;
            long count = counter.get();
            log.logStatus(MessageFormat.format("Export finished with {0} records/triples in average speed {1}/s", count, countPerSecond(count, time - startTime)));
            try {
                doClose();
            } catch (Exception e) {
                throw new ExportException(e);
            }
        }
        private double countPerSecond(long count, long millis) {
        	long secs = TimeUnit.MILLISECONDS.toSeconds(millis);
        	if (secs == 0L) {
        		return Double.POSITIVE_INFINITY;
        	} else {
        		return count/secs;
        	}
        }
        protected abstract void doClose() throws Exception;
    }

    private static class CSVResultWriter extends QueryResultWriter {

        private static final char[] HEX_DIGIT = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

        private static String escapeAndQuoteField(String field) {
            char fch[] = field.toCharArray();
            boolean quoted = fch.length == 0;
            StringBuilder sb = new StringBuilder();
            for (char c : fch) {
                if (c == '"') {
                    sb.append("\"\"");
                    quoted = true;
                } else if (c == '\n') {
                    sb.append("\\n");
                } else if (c == '\r') {
                    sb.append("\\r");
                } else if (c == '\\'){
                    sb.append("\\\\");
                } else if (c == ',') {
                    sb.append(',');
                    quoted = true;
                } else if (c < 32 || c > 126) {
                    sb.append("\\u");
                    sb.append(HEX_DIGIT[(c >> 12) & 0xF]);
                    sb.append(HEX_DIGIT[(c >>  8) & 0xF]);
                    sb.append(HEX_DIGIT[(c >>  4) & 0xF]);
                    sb.append(HEX_DIGIT[ c        & 0xF]);
                } else {
                    sb.append(c);
                }
            }
            if (quoted) {
                return "\"" + sb.toString() + "\"";
            } else {
                return sb.toString();
            }
        }

        private final Writer writer;

        CSVResultWriter(StatusLog log, OutputStream out) {
            super(log);
            this.writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        }

        @Override
        public void writeTupleQueryResult(TupleQueryResult queryResult) throws IOException {
            List<String> bns = queryResult.getBindingNames();
            boolean first = true;
            for (String bn : bns) {
                if (first) {
                    first = false;
                } else {
                    writer.write(',');
                }
                writer.write(escapeAndQuoteField(bn));
            }
            writer.write('\n');
            while (queryResult.hasNext()) {
                BindingSet bs = queryResult.next();
                first = true;
                for (String bn : bns) {
                    if (first) {
                        first = false;
                    } else {
                        writer.write(',');
                    }
                    Value v = bs.getValue(bn);
                    if (v != null) {
                        writer.write(escapeAndQuoteField(v.stringValue()));
                    }
                }
                writer.write('\n');
                tick();
            }
        }

        @Override
        public void writeGraphQueryResult(GraphQueryResult queryResult) throws IOException {
            throw new ExportException("CSV format does not support graph query results.");
        }

        @Override
        public void doClose() throws IOException {
            writer.close();
        }
    }

    private static class RIOResultWriter extends QueryResultWriter {

        private final OutputStream out;
        private final RDFWriter writer;

        RIOResultWriter(StatusLog log, RDFFormat rdfFormat, OutputStream out) {
            super(log);
            this.out = out;
            this.writer = Rio.createWriter(rdfFormat, out);
        }

        @Override
        public void writeTupleQueryResult(TupleQueryResult queryResult) throws IOException {
            throw new ExportException(String.format("%s format does not support tuple query results.", writer.getRDFFormat().getName()));
        }

        @Override
        public void writeGraphQueryResult(GraphQueryResult queryResult) throws IOException {
            writer.startRDF();
            for (Map.Entry<String, String> me : queryResult.getNamespaces().entrySet()) {
                writer.handleNamespace(me.getKey(), me.getValue());
            }
            while (queryResult.hasNext()) {
                writer.handleStatement(queryResult.next());
                tick();
            }
            writer.endRDF();
        }

        @Override
        public void doClose() throws IOException {
            out.close();
        }
    }

    private static class JDBCResultWriter extends QueryResultWriter {

        private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[a-zA-Z_0-9\\.]+$");
        private static final Collection<Integer> DATE_TIME_TYPES = Arrays.asList(Types.DATE, Types.TIME, Types.TIMESTAMP);

        private final Connection con;
        private final String tableName;
        private final boolean trimTable;

        JDBCResultWriter(StatusLog log, String dbUrl, String tableName, String[] connProps, final String driverClass, URL[] driverClasspath, boolean trimTable) throws IOException {
            super(log);
            this.trimTable = trimTable;
            try {
                this.tableName = tableName;
                if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                    throw new ExportException("Illegal character(s) in table name: " + tableName);
                }
                final ArrayList<URL> urls = new ArrayList<>();
                if (driverClasspath != null) {
                    urls.addAll(Arrays.asList(driverClasspath));
                }
                Driver driver = AccessController.doPrivileged(new PrivilegedExceptionAction<Driver>() {
                    @Override
                    public Driver run() throws ReflectiveOperationException {
                        return (Driver)Class.forName(driverClass, true, new URLClassLoader(urls.toArray(new URL[urls.size()]))).getDeclaredConstructor().newInstance();
                    }
                });
                Properties props = new Properties();
                if (connProps != null) {
                    for (String p : connProps) {
                        int i = p.indexOf('=');
                        if (i < 0) {
                            props.put(p, "true");
                        } else {
                            props.put(p.substring(0, i), p.substring(i + 1));
                        }
                    }
                }
                this.con = driver.connect(dbUrl, props);
            } catch (SQLException | PrivilegedActionException e) {
                throw new ExportException(e);
            }
        }

        @Override
        public void writeTupleQueryResult(TupleQueryResult queryResult) throws IOException {
            try {
                List<String> bns = queryResult.getBindingNames();
                if (bns.size() < 1) return;
                con.setAutoCommit(false);
                if (trimTable) try (Statement s = con.createStatement()) {
                    s.execute("delete from " + tableName);
                }
                StringBuilder sb = new StringBuilder("select ").append(bns.get(0));
                for (int i = 1; i < bns.size(); i++) {
                    sb.append(',').append(bns.get(i));
                }
                sb.append(" from ").append(tableName);
                int columnTypes[] = new int[bns.size()];
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(sb.toString())) {
                        ResultSetMetaData meta = rs.getMetaData();
                        for (int i=0; i<meta.getColumnCount(); i++) {
                            columnTypes[i] = meta.getColumnType(i+1);
                        }
                    }
                }
                sb = new StringBuilder("insert into ").append(tableName).append(" (").append(bns.get(0));
                for (int i = 1; i < bns.size(); i++) {
                    sb.append(',').append(bns.get(i));
                }
                sb.append(") values (?");
                for (int i = 1; i < bns.size(); i++) {
                    sb.append(",?");
                }
                sb.append(')');
                try (PreparedStatement ps = con.prepareStatement(sb.toString())) {
                    while (queryResult.hasNext()) {
                        BindingSet bs = queryResult.next();
                        for (int i=0; i < bns.size(); i++) {
                            String bn = bns.get(i);
                            Value v = bs.getValue(bn);
                            if (v instanceof Literal && DATE_TIME_TYPES.contains(columnTypes[i])) {
                                ps.setTimestamp(i+1, new Timestamp(((Literal)v).calendarValue().toGregorianCalendar().getTimeInMillis()));
                            } else if (v instanceof Literal && columnTypes[i] == Types.FLOAT) {
                                ps.setFloat(i+1, ((Literal)v).floatValue());
                            } else if (v instanceof Literal && columnTypes[i] == Types.DOUBLE) {
                                ps.setDouble(i+1, ((Literal)v).doubleValue());
                            } else {
                                ps.setObject(i+1, v == null ? null : v.stringValue(), columnTypes[i]);
                            }
                        }
                        ps.addBatch();
                        if (tick() % 1000 == 0) {
                            for (int i : ps.executeBatch()) {
                                if (i != 1) {
                                	throw new SQLException("Row has not been inserted for uknown reason");
                                }
                            }
                        }
                    }
                    for (int i : ps.executeBatch()) {
                        if (i != 1) {
                        	throw new SQLException("Row has not been inserted for uknown reason");
                        }
                    }
                }
                con.commit();
            } catch (SQLException e) {
                throw new ExportException(e);
            }
        }

        @Override
        public void writeGraphQueryResult(GraphQueryResult queryResult) throws IOException {
            throw new ExportException("JDBC does not support graph query results.");
        }

        @Override
        public void doClose() throws SQLException {
            con.close();
        }
    }

    static class ElasticsearchWriter extends QueryResultWriter {
    	private final String indexName;
    	private final ElasticsearchTransport esTransport;
    	private final ElasticsearchClient esClient;
    	private final RDFFactory rdfFactory;
    	private final ValueFactory valueFactory;
    	private final int batchSize = 1000;

    	public ElasticsearchWriter(StatusLog log, ElasticSettings esSettings, RDFFactory rdfFactory, ValueFactory valueFactory) throws IOException, GeneralSecurityException {
    		super(log);
    		this.indexName = esSettings.getIndexName();
    		this.esTransport = esSettings.createTransport();
    		this.esClient = new ElasticsearchClient(this.esTransport);
    		this.rdfFactory = rdfFactory;
    		this.valueFactory = valueFactory;
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

        	for (List<BindingSet> bsets : Iterables.partition(queryResult, batchSize)) {
        		List<BulkOperation> ops = new ArrayList<>(bsets.size());
    			for (BindingSet bs : bsets) {
	    			Map<String,Object> doc = new HashMap<>(auxBindingNames.size()+3);
	    			MapDocumentWriter writer = new MapDocumentWriter(rdfFactory, valueFactory, doc);
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
		    			ops.add(BulkOperation.of(opf -> opf.create(idxf -> idxf.id(id).document(doc))));
		    			tick();
	    			}
    			}

	    		BulkResponse resp = esClient.bulk(bulkf -> bulkf.index(indexName).operations(ops));
	    		if (resp.errors()) {
	    			StringBuilder errs = new StringBuilder();
	    			for (BulkResponseItem item : resp.items()) {
	    				ErrorCause err = item.error();
	    				if (err != null) {
	    					errs.append(err.type()).append(": ").append(err.reason()).append("\n");
	    				}
	    			}
	    			throw new ExportException("There were errors with exporting to Elasticsearch:\n"+errs);
	    		}
        	}
        }

        @Override
        public void writeGraphQueryResult(GraphQueryResult queryResult) throws IOException {
            throw new ExportException("Elasticsearch does not support graph query results.");
        }

        @Override
        public void doClose() throws IOException {
        	esTransport.close();
        }
    }

    private static class MapDocumentWriter extends HalyardElasticIndexer.DocumentWriter {
    	private final Map<String,Object> map;

    	MapDocumentWriter(RDFFactory rdfFactory, ValueFactory valueFactory, Map<String,Object> map) {
			super(rdfFactory, valueFactory);
			this.map = map;
		}

		@Override
		public void writeNumericField(String key, Number value) throws IOException {
			map.put(key, value);
		}

		@Override
		public void writeStringField(String key, String value) throws IOException {
			map.put(key, value);
		}

		@Override
		public void writeArrayField(String key, Object[] value) throws IOException {
			map.put(key, value);
		}

		@Override
		public void close() throws IOException {
			// do nothing
		}
    }

    private static class NullResultWriter extends QueryResultWriter {

        public NullResultWriter(StatusLog log) {
            super(log);
        }

        @Override
        public void writeTupleQueryResult(TupleQueryResult queryResult) throws IOException {
            while (queryResult.hasNext()) {
                queryResult.next();
                tick();
            }
        }

        @Override
        public void writeGraphQueryResult(GraphQueryResult queryResult) throws IOException {
            while (queryResult.hasNext()) {
                queryResult.next();
                tick();
            }
        }

        @Override
        protected void doClose() {
        }
    }

    /**
     * Export function is called for the export execution with given arguments.
     */
    static void export(Repository repo, String query, QueryResultWriter writer) throws IOException {
        writer.initTimer();
        writer.log.logStatus("Query execution started");
        try(RepositoryConnection conn = repo.getConnection()) {
            Query q = conn.prepareQuery(QueryLanguage.SPARQL, query);
            if (q instanceof TupleQuery) {
            	try (TupleQueryResult queryResult = ((TupleQuery) q).evaluate()) {
            		writer.writeTupleQueryResult(queryResult);
            	}
            } else if (q instanceof GraphQuery) {
            	try (GraphQueryResult queryResult = ((GraphQuery) q).evaluate()) {
            		writer.writeGraphQueryResult(queryResult);
            	}
            } else {
                throw new ExportException("Only SPARQL Tuple and Graph query types are supported.");
            }
            writer.log.logStatus("Export finished");
        }
    }

    static QueryResultWriter createWriter(Configuration conf, StatusLog log, String targetUrl, RDFFactory rdfFactory, ValueFactory valueFactory, String driverClass, String driverClasspath, String[] jdbcProperties, boolean trimTable) throws IOException {
        if (targetUrl.startsWith("null:")) {
            return new NullResultWriter(log);
        } else if (targetUrl.startsWith("jdbc:")) {
            int i = targetUrl.lastIndexOf('/');
            if (i < 0) {
            	throw new ExportException("Taret URL does not end with /<table_name>");
            }
            if (driverClass == null) {
            	throw new ExportException("Missing mandatory JDBC driver class name argument -c <driver_class>");
            }
            URL driverCP[] = null;
            if (driverClasspath != null) {
                String jars[] = driverClasspath.split(":");
                driverCP = new URL[jars.length];
                for (int j=0; j<jars.length; j++) {
                    File f = new File(jars[j]);
                    if (!f.isFile()) {
                    	throw new ExportException("Invalid JDBC driver classpath element: " + jars[j]);
                    }
                    driverCP[j] = f.toURI().toURL();
                }
            }
            return new JDBCResultWriter(log, targetUrl.substring(0, i), targetUrl.substring(i+1), jdbcProperties, driverClass, driverCP, trimTable);
        } else if (isElasticsearch(targetUrl)) {
    		ElasticSettings esSettings = ElasticSettings.from(targetUrl, conf);
    		try {
    			return new ElasticsearchWriter(log, esSettings, rdfFactory, valueFactory);
    		} catch (GeneralSecurityException e) {
    			throw new ExportException(e);
    		}
        } else {
	    	FileSystem fileSystem = FileSystem.get(URI.create(targetUrl), conf);
            OutputStream out = fileSystem.create(new Path(targetUrl));
            try {
                if (targetUrl.endsWith(".bz2")) {
                    out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, out);
                    targetUrl = targetUrl.substring(0, targetUrl.length() - 4);
                } else if (targetUrl.endsWith(".gz")) {
                    out = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, out);
                    targetUrl = targetUrl.substring(0, targetUrl.length() - 3);
                }
            } catch (CompressorException e) {
                IOUtils.closeQuietly(out);
                throw new ExportException(e);
            }
            if (targetUrl.endsWith(".csv")) {
                return new CSVResultWriter(log, out);
            } else {
            	final String formatUrl = targetUrl;
                Optional<RDFFormat> format = Rio.getWriterFormatForFileName(formatUrl);
                return new RIOResultWriter(log, format.orElseThrow(() -> new ExportException("Unsupported target file format extension: " + formatUrl)), out);
            }
        }
    }

    static boolean isElasticsearch(String targetUrl) {
    	return targetUrl.startsWith("http:") || targetUrl.startsWith("https:");
    }

    private static String listRDFOut() {
        StringBuilder sb = new StringBuilder();
        for (RDFFormat fmt : RDFWriterRegistry.getInstance().getKeys()) {
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

    public HalyardExport() {
        super(
            "export",
            "Halyard Export is a command-line application designed to export data from HBase (a Halyard dataset) into various targets and formats.",
            "The exported data is determined by a SPARQL query. It can be either a SELECT query that produces a set of tuples (a table) or a CONSTRUCT/DESCRIBE query that produces a set of triples (a graph). "
                + "The supported target systems, query types, formats, and compressions are listed in the following table:\n"
                + "+---------------+--------------+---------------------------------+---------------------------------------+\n"
                + "| Target        | Protocol     | SELECT query                    | CONSTRUCT/DESCRIBE query              |\n"
                + "+---------------+--------------+---------------------------------+---------------------------------------+\n"
                + "| Local FS      | file:        | .csv + compression              | RDF4J supported formats + compression |\n"
                + "| Hadoop FS     | hdfs:        | .csv + compression              | RDF4J supported formats + compression |\n"
                + "| Database      | jdbc:        | direct mapping to table columns | not supported                         |\n"
                + "| Elasticsearch | http: https: | direct mapping to fields        | not supported                         |\n"
                + "| Dry run       | null:        | .csv + compression              | RDF4J supported formats + compression |\n"
                + "+---------------+--------------+---------------------------------+---------------------------------------+\n"
                + "Other Hadoop standard and optional filesystems (like s3:, s3n:, file:, ftp:, webhdfs:) may work according to the actual cluster configuration, however they have not been tested.\n"
                + "Optional compressions are:\n"
                + "* Bzip2 (.bz2)\n"
                + "* Gzip (.gz)\n"
                + "The RDF4J supported RDF formats are:\n"
                + listRDFOut()
                + "Example: halyard export -s my_dataset -q 'select * where {?subjet ?predicate ?object}' -t hdfs:/my_folder/my_data.csv.gz"
        );
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", true, true);
        addOption("q", "query", "sparql_query", "SPARQL tuple or graph query executed to export the data", true, true);
        addOption("t", "target-url", "target_url", "file://<path>/<file_name>.<ext> or hdfs://<path>/<file_name>.<ext> or jdbc:<jdbc_connection>/<table_name>", true, true);
        addOption("p", "jdbc-property", "property=value", "JDBC connection property, the most frequent JDBC connection properties are -p user=<jdbc_connection_username> and -p password=<jdbc_connection_password>`", false, false);
        addOption("l", "jdbc-driver-classpath", "driver_classpath", "JDBC driver classpath delimited by ':'", false, true);
        addOption("c", "jdbc-driver-class", "driver_class", "JDBC driver class name, mandatory for JDBC export", false, true);
        addOption("r", "trim", null, "Trim target table before export (apply for JDBC only)", false, false);
        addOption("i", "elastic-index", "elastic_index_url", ElasticSettings.ELASTIC_INDEX_URL, "Optional ElasticSearch index URL", false, true);
    }

    @Override
    protected int run(CommandLine cmd) throws Exception {
    	String source = cmd.getOptionValue('s');
    	String query = cmd.getOptionValue('q');
        configureString(cmd, 'i', null);
        StatusLog log = new StatusLog() {
            @Override
            public void tick() {}

            @Override
            public void logStatus(String status) {
                LOG.info(status);
            }
        };
    	HBaseSail sail = new HBaseSail(getConf(), source, false, 0, true, 0, ElasticSettings.from(getConf()), null);
    	HBaseRepository repo = new HBaseRepository(sail);
    	repo.init();
    	try {
	    	try (QueryResultWriter writer = createWriter(sail.getConfiguration(), log, cmd.getOptionValue('t'), sail.getRDFFactory(), sail.getValueFactory(), cmd.getOptionValue('c'), cmd.getOptionValue('l'), cmd.getOptionValues('p'), cmd.hasOption('r'))) {
	    		export(repo, query, writer);
	    	}
    	} finally {
    		repo.shutDown();
    	}
        return 0;
    }
}
