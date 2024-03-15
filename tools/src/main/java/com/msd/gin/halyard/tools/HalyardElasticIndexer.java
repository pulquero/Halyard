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

import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.SSLSettings;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.model.TermRole;
import com.msd.gin.halyard.model.TupleLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.sail.search.SearchDocument;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype.GEO;
import org.eclipse.rdf4j.model.base.CoreDatatype.XSD;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce tool indexing all RDF literals in Elasticsearch
 * @author Adam Sotona (MSD)
 */
public final class HalyardElasticIndexer extends AbstractHalyardTool {
    private static final Logger LOG = LoggerFactory.getLogger(HalyardElasticIndexer.class);

    private static final String TOOL_NAME = "esindex";

	private static final String INDEX_URL_PROPERTY = confProperty(TOOL_NAME, "index.url");
	private static final String CREATE_INDEX_PROPERTY = confProperty(TOOL_NAME, "index.create");
	private static final String PREDICATE_PROPERTY = confProperty(TOOL_NAME, "property");
	private static final String NAMED_GRAPH_PROPERTY = confProperty(TOOL_NAME, "named-graph");
	private static final String ALIAS_PROPERTY = confProperty(TOOL_NAME, "alias");
    private static final long STATUS_UPDATE_INTERVAL = 100000L;

	enum Counters {
		INDEXED_LITERALS
	}

    static final class IndexerMapper extends RdfTableMapper<NullWritable, Text>  {

        final Text outputJson = new Text();
        boolean hasGeometryField;
        long counter = 0, exports = 0, statements = 0;
        StatementIndex<?,?,?,?> lastIndex;
        Set<Value> lastLiterals;
        byte[] lastHash;
        int hashOffset;
        int hashLen;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            hasGeometryField = getFieldConfigProperty(conf, SearchDocument.GEOMETRY_FIELD);
            openKeyspace(conf, conf.get(SOURCE_NAME_PROPERTY), conf.get(SNAPSHOT_PATH_PROPERTY));
        }

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result value, Context output) throws IOException, InterruptedException {
        	byte[] key = rowKey.get();
            StatementIndex<?,?,?,?> index = stmtIndices.toIndex(key[rowKey.getOffset()]);
            if (index != lastIndex) {
            	lastIndex = index;
            	lastHash = new byte[0];
            	hashOffset = index.getName().isQuadIndex() ? 1 + index.getRole(TermRole.CONTEXT).keyHashSize() : 1;
                hashLen = index.getRole(TermRole.OBJECT).keyHashSize();
            }

            if (!Arrays.equals(key, hashOffset, hashOffset + hashLen, lastHash, 0, lastHash.length)) {
            	if (lastHash.length != hashLen) {
            		lastHash = new byte[hashLen];
            	}
            	System.arraycopy(key, hashOffset, lastHash, 0, hashLen);
            	lastLiterals = new HashSet<>();
            }

            Statement[] stmts = stmtIndices.parseStatements(null, null, null, null, value, vf);
            for (Statement st : stmts) {
                statements++;
            	Literal l = (Literal) st.getObject();
                if (lastLiterals.add(l)) {
	                JsonDocumentWriter writer = new JsonDocumentWriter(rdfFactory, vf);
	                if (writer.writeValue(l) != null) {
		    			if (hasGeometryField && l.getCoreDatatype() == GEO.WKT_LITERAL) {
		    				writer.writeStringField(SearchDocument.GEOMETRY_FIELD, l.getLabel());
		    			}
		    			writer.close();
		                outputJson.set(writer.toString());
		                output.write(NullWritable.get(), outputJson);
		                exports++;
	                }
                }
            }

            if ((counter++ % STATUS_UPDATE_INTERVAL) == 0) {
                output.setStatus(MessageFormat.format("{0} st:{1} exp:{2} ", counter, statements, exports));
            }
        }

        @Override
        protected void cleanup(Context output) throws IOException {
        	closeKeyspace();
        	output.getCounter(Counters.INDEXED_LITERALS).setValue(exports);
        }
    }

    public HalyardElasticIndexer() {
        super(
            TOOL_NAME,
            "Halyard ElasticSearch Index is a MapReduce application that indexes all literals in the given dataset into a supplementary ElasticSearch server/cluster. "
                + "A Halyard repository configured with such supplementary ElasticSearch index can then provide more advanced text search features over the indexed literals.",
            "Default index configuration is:\n"
            + getMappingConfig("\u00A0", "2*<num_of_region_servers>", "1", null)
            + "Example: halyard esindex -s my_dataset -t http://my_elastic.my.org:9200/my_index [-g 'http://whatever/graph']"
        );
        addOption("s", "source-dataset", "dataset_table", SOURCE_NAME_PROPERTY, "Source HBase table with Halyard RDF store", true, true);
        addOption("t", "target-index", "target_url", INDEX_URL_PROPERTY, "Elasticsearch target index url <server>:<port>/<index_name>", true, true);
        addOption("c", "create-index", null, CREATE_INDEX_PROPERTY, "Optionally create Elasticsearch index", false, true);
        addOption("p", "predicate", "predicate", PREDICATE_PROPERTY, "Optionally restrict indexing to the given predicate only", false, true);
        addOption("g", "named-graph", "named_graph", NAMED_GRAPH_PROPERTY, "Optionally restrict indexing to the given named graph only", false, true);
        addOption("u", "restore-dir", "restore_folder", SNAPSHOT_PATH_PROPERTY, "If specified then -s is a snapshot name and this is the restore folder on HDFS", false, true);
        addOption("a", "alias", "alias", ALIAS_PROPERTY, "If creating an index, optionally add it to an alias", false, true);
    }

    private static String getMappingConfig(String linePrefix, String shards, String replicas, String alias) {
        String config =
                  linePrefix + "{\n"
                + linePrefix + "    \"mappings\" : {\n"
                + linePrefix + "        \"properties\" : {\n"
                + linePrefix + "            \"" + SearchDocument.ID_FIELD + "\" : { \"type\" : \"keyword\", \"index\" : false },\n"
                + linePrefix + "            \"" + SearchDocument.LABEL_FIELD + "\" : { \"type\" : \"text\", \"fields\" : {"
                							// NB: floating-point values aren't guaranteed to have an exact representation so coerce them from string instead
                + linePrefix + "                \"" + SearchDocument.NUMBER_SUBFIELD + "\" : { \"type\" : \"double\", \"coerce\" : true, \"ignore_malformed\" : true },\n"
                							// use exact integer representation if available
                + linePrefix + "                \"" + SearchDocument.INTEGER_SUBFIELD + "\" : { \"type\" : \"long\", \"coerce\" : false, \"ignore_malformed\" : true },\n"
                + linePrefix + "                \"" + SearchDocument.POINT_SUBFIELD + "\" : { \"type\" : \"geo_point\", \"ignore_malformed\" : true }\n"
                + linePrefix + "                }\n"
                + linePrefix + "            },\n"
                + linePrefix + "            \"" + SearchDocument.GEOMETRY_FIELD + "\" : { \"type\" : \"geo_shape\" },\n"
                + linePrefix + "            \"" + SearchDocument.DATATYPE_FIELD + "\" : { \"type\" : \"keyword\", \"index\" : false },\n"
                + linePrefix + "            \"" + SearchDocument.LANG_FIELD + "\" : { \"type\" : \"keyword\", \"index\" : false }\n"
                + linePrefix + "        }\n"
                + linePrefix + "    },\n";
    if (alias != null) {
        config += linePrefix + "    \"aliases\" : {\n"
                + linePrefix + "         \""+alias+"\" : {}\n"
                + linePrefix + "    },\n";
    }
        config += linePrefix + "   \"settings\": {\n"
                + linePrefix + "       \"index.query.default_field\": \"label\",\n"
                + linePrefix + "       \"refresh_interval\": \"1h\",\n"
                + linePrefix + "       \"number_of_shards\": " + shards + ",\n"
                + linePrefix + "       \"number_of_replicas\": " + replicas + "\n"
                + linePrefix + "    }\n"
                + linePrefix + "}\n";
        return config;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        configureString(cmd, 's', null);
        configureString(cmd, 't', null, indexUrl -> {
        	if (indexUrl.endsWith("/")) {
        		throw new IllegalArgumentException("The index URL should end with the index name not a /");
        	}
        });
        configureString(cmd, 'u', null);
        configureBoolean(cmd, 'c');
        configureString(cmd, 'a', null);
        configureIRI(cmd, 'p', null);
        configureIRI(cmd, 'g', null);
        String source = getConf().get(SOURCE_NAME_PROPERTY);
        String target = getConf().get(INDEX_URL_PROPERTY);
        URL targetUrl = new URL(target);
        String indexName = targetUrl.getPath().substring(1);
        boolean createIndex = getConf().getBoolean(CREATE_INDEX_PROPERTY, false);
        String snapshotPath = getConf().get(SNAPSHOT_PATH_PROPERTY);
        if (snapshotPath != null) {
			FileSystem fs = CommonFSUtils.getRootDirFileSystem(getConf());
        	if (fs.exists(new Path(snapshotPath))) {
        		throw new IOException("Snapshot restore directory already exists");
        	}
        }
        String predicate = getConf().get(PREDICATE_PROPERTY);
        String namedGraph = getConf().get(NAMED_GRAPH_PROPERTY);

        getConf().set("es.nodes", targetUrl.getHost()+":"+targetUrl.getPort());
        getConf().set("es.resource", indexName);
        getConf().set("es.mapping.id", SearchDocument.ID_FIELD);
        getConf().set("es.input.json", "yes");
        getConf().setIfUnset("es.batch.size.bytes", Integer.toString(5*1024*1024));
        getConf().setIfUnset("es.batch.size.entries", Integer.toString(10000));

        if (createIndex) {
        	createIndex(targetUrl);
        }

        // retrieve mapping config to use
        JSONObject mapping = getIndexMapping(targetUrl);
    	JSONObject fields = mapping.getJSONObject(indexName).getJSONObject("mappings").getJSONObject("properties");
    	for (String field : (Set<String>) fields.keySet()) {
    		getConf().setBoolean(confProperty(TOOL_NAME, "fields."+field), true);
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
        if (System.getProperty("exclude.es-hadoop") == null) {
         	TableMapReduceUtil.addDependencyJarsForClasses(getConf(), EsOutputFormat.class);
        }
        HBaseConfiguration.addHbaseResources(getConf());
        Job job = Job.getInstance(getConf(), "HalyardElasticIndexer " + source + " -> " + target);
        job.setJarByClass(HalyardElasticIndexer.class);
        TableMapReduceUtil.initCredentials(job);

        RDFFactory rdfFactory;
        Keyspace keyspace = getKeyspace(source, snapshotPath);
        try {
       		rdfFactory = loadRDFFactory(keyspace);
		} finally {
			keyspace.close();
		}
        StatementIndices indices = new StatementIndices(getConf(), rdfFactory);
        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        IRI predicateIRI = (predicate != null) ? vf.createIRI(predicate) : null;
        Resource graphIRI = (namedGraph != null) ? vf.createIRI(namedGraph) : null;
        Scan scan = indices.scanLiterals(predicateIRI, graphIRI);
        keyspace.initMapperJob(
            scan,
            IndexerMapper.class,
            NullWritable.class,
            Text.class,
            job);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setNumReduceTasks(0);
        job.setSpeculativeExecution(false);
	    try {
	        if (job.waitForCompletion(true)) {
	            refreshIndex(targetUrl);
	            LOG.info("Elastic indexing completed.");
	            return 0;
	        } else {
	    		LOG.error("Elastic indexing failed to complete.");
	            return -1;
	        }
        } finally {
        	keyspace.destroy();
        }
    }

    private void configureAuth(HttpURLConnection http) {
        String esUser = getConf().get("es.net.http.auth.user");
        if (esUser != null) {
        	String esPassword = getConf().get("es.net.http.auth.pass");
        	String userPass = esUser + ':' + esPassword;
        	String basicAuth = "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes(StandardCharsets.UTF_8));
        	http.setRequestProperty("Authorization", basicAuth);
        }
    }

	private void configureSSL(HttpsURLConnection https) throws IOException, GeneralSecurityException {
		SSLSettings sslSettings = SSLSettings.from(getConf());
		SSLContext sslContext = sslSettings.createSSLContext();
		https.setSSLSocketFactory(sslContext.getSocketFactory());
	}

	private HttpURLConnection open(URL url) throws IOException, GeneralSecurityException {
        HttpURLConnection http = (HttpURLConnection)url.openConnection();
        configureAuth(http);
        if (http instanceof HttpsURLConnection) {
        	HttpsURLConnection https = (HttpsURLConnection) http;
        	configureSSL(https);
        }
        return http;
	}

	private void createIndex(URL indexUrl) throws IOException, GeneralSecurityException {
        int shards;
        int replicas = 1;
        try (Connection conn = ConnectionFactory.createConnection(getConf())) {
            try (Admin admin = conn.getAdmin()) {
                shards = admin.getRegionServers().size();
            }
        }
        HttpURLConnection http = open(indexUrl);
        http.setRequestMethod("PUT");
        http.setDoOutput(true);
        http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        String alias = getConf().get(ALIAS_PROPERTY);
        byte b[] = Bytes.toBytes(getMappingConfig("", Integer.toString(shards), Integer.toString(replicas), alias));
        http.setFixedLengthStreamingMode(b.length);
        http.connect();
        try {
            try (OutputStream post = http.getOutputStream()) {
                post.write(b);
            }
            int response = http.getResponseCode();
            if (response == HttpURLConnection.HTTP_OK) {
                LOG.info("Elastic index succesfully configured.");
            } else {
                String msg = http.getResponseMessage();
                String resp = getHttpError(http);
                LOG.warn("Elastic index responded with {}: {}", response, resp);
                boolean alreadyExist = false;
                if (response == HttpURLConnection.HTTP_BAD_REQUEST) {
                	try {
                        alreadyExist = new JSONObject(resp).getJSONObject("error").getString("type").contains("exists");
                    } catch (Exception ex) {
                        //ignore
                    }
                }
                if (!alreadyExist) {
                	throw new IOException(msg);
                }
            }
        } finally {
            http.disconnect();
        }
	}

	private JSONObject getIndexMapping(URL indexUrl) throws IOException, GeneralSecurityException {
		JSONObject mapping;
        HttpURLConnection http = open(new URL(indexUrl + "/_mapping"));
        http.connect();
        try {
	        int response = http.getResponseCode();
	        if (response != HttpURLConnection.HTTP_OK) {
	            String msg = http.getResponseMessage();
	            String resp = getHttpError(http);
	            LOG.warn("Elastic index responded with {}: {}", response, resp);
	            throw new IOException(msg);
	        }
	    	mapping = new JSONObject(IOUtils.toString(http.getInputStream(), StandardCharsets.UTF_8));
        } finally {
            http.disconnect();
        }
        return mapping;
	}

	private void refreshIndex(URL indexUrl) throws IOException, GeneralSecurityException {
        HttpURLConnection http = open(new URL(indexUrl + "/_refresh"));
        http.setRequestMethod("POST");
        http.connect();
        try {
	        int response = http.getResponseCode();
	        if (response != HttpURLConnection.HTTP_OK) {
	            String msg = http.getResponseMessage();
	            String resp = getHttpError(http);
	            LOG.warn("Elastic index responded with {}: {}", response, resp);
	            throw new IOException(msg);
	        }
        } finally {
            http.disconnect();
        }
        LOG.info("Elastic index refreshed.");
	}

	private static String getHttpError(HttpURLConnection http) throws IOException {
        InputStream errStream = http.getErrorStream();
        return (errStream != null) ? IOUtils.toString(errStream, StandardCharsets.UTF_8) : "";
	}

	private static boolean getFieldConfigProperty(Configuration conf, String fieldName) {
		return conf.getBoolean(confProperty(TOOL_NAME, "fields."+fieldName), false);
	}


	static abstract class DocumentWriter implements Closeable {
		private final RDFFactory rdfFactory;
		private final ValueFactory valueFactory;

		DocumentWriter(RDFFactory rdfFactory, ValueFactory valueFactory) {
			this.rdfFactory = rdfFactory;
			this.valueFactory = valueFactory;
		}

		public final String writeValue(Value v) throws IOException {
			if (!v.isIRI() && !v.isLiteral()) {
				return null;
			}
			String id = rdfFactory.id(v).toString();
			writeStringField(SearchDocument.ID_FIELD, id);
			if (v.isIRI()) {
				writeStringField(SearchDocument.IRI_FIELD, v.stringValue());
			} else {
				Literal l = (Literal) v;
	        	Optional<XSD> xsd = l.getCoreDatatype().asXSDDatatype();
	   			if (xsd.map(XSD::isIntegerDatatype).orElse(false)) {
					// use exact integer representation if available
					// NB: floating-point values aren't guaranteed to have an exact representation so coerce them from string instead
	   				try {
	   					writeNumericField(SearchDocument.LABEL_FIELD, l.longValue());
	   				} catch (NumberFormatException nfe) {
	   					writeStringField(SearchDocument.LABEL_FIELD, l.getLabel());
	   				}
	    		} else {
	    			writeStringField(SearchDocument.LABEL_FIELD, l.getLabel());
	    		}
				writeStringField(SearchDocument.DATATYPE_FIELD, l.getDatatype().stringValue());
				String langTag = l.getLanguage().orElse(null);
	            if(langTag != null) {
					writeStringField(SearchDocument.LANG_FIELD, langTag);
	            }
			}
			return id;
		}

		public final void writeObjectField(String key, Value v) throws IOException {
			Object o = toObject(v);
			if (o instanceof Number) {
				writeNumericField(key, (Number) v);
			} else if (o instanceof Object[]) {
				writeArrayField(key, (Object[]) o);
			} else if (o instanceof String) {
				writeStringField(key, (String) o);
			}
		}

		private Object toObject(Value v) throws IOException {
			if (v != null) {
				if (v.isLiteral()) {
					Literal l = (Literal) v;
		    		Optional<XSD> xsd = l.getCoreDatatype().asXSDDatatype();
		    		if (xsd.map(XSD::isNumericDatatype).orElse(false)) {
		   				try {
			    			if (xsd.map(XSD::isIntegerDatatype).orElse(false)) {
			    				return l.longValue();
			    			} else {
			    				return l.doubleValue();
			    			}
		   				} catch (NumberFormatException nfe) {
		   	    			return l.getLabel();
		   				}
		    		} else if (HALYARD.TUPLE_TYPE.equals(l.getDatatype())) {
		    			Value[] varr = TupleLiteral.arrayValue(l, valueFactory);
		    			Object[] oarr = new Object[varr.length];
		    			for (int i=0; i<varr.length; i++) {
		    				oarr[i] = toObject(varr[i]);
		    			}
		    			return oarr;
		    		} else {
		    			return l.getLabel();
		    		}
				} else if (v.isIRI()) {
					return v.stringValue();
				}
			}
			return null;
		}

		public abstract void writeNumericField(String key, Number value) throws IOException;
		public abstract void writeStringField(String key, String value) throws IOException;
		public abstract void writeArrayField(String key, Object[] value) throws IOException;
	}

	static class JsonDocumentWriter extends DocumentWriter {
		private final StringBuilderWriter writer;
		private String sep = "";

		JsonDocumentWriter(RDFFactory rdfFactory, ValueFactory valueFactory) {
			super(rdfFactory, valueFactory);
			this.writer = new StringBuilderWriter(128);
			this.writer.append("{");
		}

		private void writeFieldName(String key) throws IOException {
            writer.append(sep).append("\"").append(key).append("\":");
            sep = ",";
		}

		@Override
		public void writeNumericField(String key, Number value) throws IOException {
			writeFieldName(key);
			writer.append(value.toString());
		}

		@Override
		public void writeStringField(String key, String value) throws IOException {
			writeFieldName(key);
            JSONObject.quote(value, writer);
		}

		@Override
		public void writeArrayField(String key, Object[] value) throws IOException {
			writeFieldName(key);
			new JSONArray(value).write(writer);
		}

		@Override
		public void close() throws IOException {
            writer.append("}\n");
    		writer.close();
		}

		@Override
		public String toString() {
			return writer.toString();
		}
	}
}
