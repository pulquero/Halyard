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

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.TableConfig;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.search.SearchDocument;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardElasticIndexerTest extends AbstractHalyardToolTest {
	private static final String ES_VERSION = "8.4.2";
	private static final String NODE_ID = UUID.randomUUID().toString();
	private static final String INDEX_NAME = "my_index";

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardElasticIndexer();
	}

	@Test
    public void testElasticIndexer() throws Exception {
		testElasticIndexer("elasticTable", HBaseServerTestInstance.getInstanceConfig());
	}

	@Test
    public void testDegenerateKeys() throws Exception {
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
    	conf.setInt(TableConfig.KEY_SIZE_SUBJECT, 1);
    	conf.setInt(TableConfig.END_KEY_SIZE_SUBJECT, 1);
    	conf.setInt(TableConfig.KEY_SIZE_PREDICATE, 1);
    	conf.setInt(TableConfig.END_KEY_SIZE_PREDICATE, 1);
    	conf.setInt(TableConfig.KEY_SIZE_OBJECT, 1);
    	conf.setInt(TableConfig.END_KEY_SIZE_OBJECT, 1);
    	conf.setInt(TableConfig.KEY_SIZE_CONTEXT, 1);
		testElasticIndexer("elasticDegenerateTable", conf);
	}

	@Test
    public void testCustomMapping() throws Exception {
		Configuration conf = HBaseServerTestInstance.getInstanceConfig();
		String tableName = "elasticCustomTable";
		createSail(tableName, conf);
		String indexName = INDEX_NAME;
    	MockElasticServer server = new MockElasticServer(indexName);
        server.start();
        // mock predefined custom mapping without geometry field
    	server.fieldDefns.put(SearchDocument.ID_FIELD, new JSONObject());
    	server.fieldDefns.put(SearchDocument.LABEL_FIELD, new JSONObject());
        server.fieldDefns.put(SearchDocument.DATATYPE_FIELD, new JSONObject());
        server.fieldDefns.put(SearchDocument.LANG_FIELD, new JSONObject());
        try {
            String[] cmdLineArgs = new String[]{"-s", tableName, "-t", server.getIndexUrl()};
            int rc = run(cmdLineArgs);
            assertEquals(0, rc);
        } finally {
            server.stop();
        }
        assertNull(server.requestUri[0]);
        // geometry field wasn't added
        assertNull(server.fieldDefns.get(SearchDocument.GEOMETRY_FIELD));
        assertEquals(server.indexPath+"/_bulk", server.requestUri[1]);
        assertEquals(server.bulkBody.toString(), 200, server.bulkBody.size());
        for (int i=0; i<server.bulkBody.size(); i+=2) {
            String id = server.bulkBody.get(i).getJSONObject("index").getString("_id");
            JSONObject fields = server.bulkBody.get(i+1);
            for (String field : (Set<String>) fields.keySet()) {
            	assertNotNull(field, server.fieldDefns.get(field));
            }
        }
        assertEquals(server.indexPath+"/_refresh", server.requestUri[2]);
	}

	private HBaseSail createSail(String tableName, Configuration conf) {
        HBaseSail sail = new HBaseSail(conf, tableName, true, 0, true, 0, null, null);
        sail.init();
        ValueFactory vf = SimpleValueFactory.getInstance();
		try (SailConnection conn = sail.getConnection()) {
			for (int i = 0; i < 100; i++) {
				conn.addStatement(vf.createIRI("http://whatever/NTsubj"), vf.createIRI("http://whatever/NTpred" + i), vf.createLiteral("whatever NT value " + i), (i % 4 == 0) ? null : vf.createIRI("http://whatever/graph#" + (i % 4)));
			}
			// add some non-literal data
			for (int i = 0; i < 100; i++) {
				conn.addStatement(vf.createIRI("http://whatever/NTsubj"), vf.createIRI("http://whatever/NTpred" + i), vf.createIRI("http://whatever/NTobj" + i), (i % 4 == 0) ? null : vf.createIRI("http://whatever/graph#" + (i % 4)));
			}
		}
		return sail;
	}

	public void testElasticIndexer(String tableName, Configuration conf) throws Exception {
		HBaseSail sail = createSail(tableName, conf);
        testElasticIndexer(tableName, false, sail);
        testElasticIndexer(tableName, true, sail);
    }

    public void testElasticIndexer(String tableName, boolean namedGraphOnly, HBaseSail sail) throws Exception {
    	MockElasticServer server = new MockElasticServer(INDEX_NAME);
        server.start();
        try {
            String[] cmdLineArgs = namedGraphOnly ?
            		new String[]{"-s", tableName, "-t", server.getIndexUrl(), "-c", "-g", "http://whatever/graph#1"}
            		: new String[]{"-s", tableName, "-t", server.getIndexUrl(), "-c"};
            int rc = run(cmdLineArgs);
            assertEquals(0, rc);
        } finally {
            server.stop();
        }
        assertEquals(server.indexPath, server.requestUri[0]);
        assertNotNull(server.fieldDefns.get(SearchDocument.ID_FIELD));
        assertNotNull(server.fieldDefns.get(SearchDocument.LABEL_FIELD));
        assertNotNull(server.fieldDefns.get(SearchDocument.DATATYPE_FIELD));
        assertNotNull(server.fieldDefns.get(SearchDocument.LANG_FIELD));
        assertEquals(server.indexPath+"/_bulk", server.requestUri[1]);
        assertEquals(server.bulkBody.toString(), (namedGraphOnly ? 50 : 200), server.bulkBody.size());
        ValueFactory vf = sail.getValueFactory();
        RDFFactory rdfFactory = sail.getRDFFactory();
        for (int i=0; i<server.bulkBody.size(); i+=2) {
            String id = server.bulkBody.get(i).getJSONObject("index").getString("_id");
            JSONObject fields = server.bulkBody.get(i+1);
            for (String field : (Set<String>) fields.keySet()) {
            	assertNotNull(field, server.fieldDefns.get(field));
            }
            Literal literal = vf.createLiteral(fields.getString(SearchDocument.LABEL_FIELD), vf.createIRI(fields.getString(SearchDocument.DATATYPE_FIELD)));
            assertEquals("Invalid hash for literal " + literal, rdfFactory.id(literal).toString(), id);
        }
        assertEquals(server.indexPath+"/_refresh", server.requestUri[2]);
    }

    @Override
    protected int run(String ... args) throws Exception {
        // fix elasticsearch classpath issues
        System.setProperty("exclude.es-hadoop", "true");
        return super.run(args);
    }


    static class MockElasticServer {
    	final String indexPath;
    	final HttpServer server;
        final String[] requestUri = new String[3];
    	final Map<String, JSONObject> fieldDefns = new HashMap<>();
        final List<JSONObject> bulkBody = new ArrayList<>(200);

        MockElasticServer(String indexName) throws IOException {
        	indexPath = "/" + indexName;
	        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
	        server.createContext("/", new HttpHandler() {
	            @Override
	            public void handle(HttpExchange he) throws IOException {
	            	String path = he.getRequestURI().getPath();
	            	int statusCode;
	            	JSONObject response;
	                if ("GET".equalsIgnoreCase(he.getRequestMethod())) {
	                	switch (path) {
	                		case "/":
			                    JSONObject version = new JSONObject();
			                    version.put("number", ES_VERSION);
			                    version.put("lucene_version", "8.11.1");
			                    version.put("minimum_wire_compatibility_version", "6.8.0");
			                    version.put("minimum_index_compatibiltiy_version", "6.0.0-beta1");
			                    statusCode = HttpURLConnection.HTTP_OK;
			                    response = new JSONObject();
			                    response.put("name", "localhost");
			                    response.put("cluster_name", "halyard-test");
			                    response.put("cluster_uuid", "_na_");
			                    response.put("version", version);
			                    response.put("tagline", "You Know, for Search");
			                    response.put("build_flavor", "default");
			                    he.getResponseHeaders().set("X-elastic-product", "Elasticsearch");
			                    break;
	                		case "/_nodes/http":
	                			JSONObject nodeInfo = new JSONObject();
	                			nodeInfo.put("version", ES_VERSION);
	                			nodeInfo.put("name", "test-es-node");
	                			nodeInfo.put("host", "localhost");
	                			nodeInfo.put("ip", server.getAddress().getAddress().getHostAddress());
	                			nodeInfo.put("roles", Arrays.asList("master", "data", "ingest"));
	                			JSONObject httpNode = new JSONObject();
	                			httpNode.put("publish_address", server.getAddress().toString());
	                			nodeInfo.put("http", httpNode);
	                			JSONObject node = new JSONObject();
	                			node.put(NODE_ID, nodeInfo);
			                    statusCode = HttpURLConnection.HTTP_OK;
			                    response = new JSONObject();
	                			response.put("nodes", node);
	                			break;
	                		default:
			                    statusCode = HttpURLConnection.HTTP_NOT_FOUND;
	                			response = null;
	                	}
	                } else {
	                	statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
	                	response = null;
	                }
	
	            	if (statusCode >= 400) {
	                	he.sendResponseHeaders(statusCode, 0);
	                    try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
	                        writer.write(he.getRequestMethod()+" "+he.getRequestURI());
	                    }
	            	} else {
	                	he.sendResponseHeaders(statusCode, response != null ? 0 : -1);
		                if (response != null) {
		                    try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
		                        response.write(writer);
		                    }
		                }
	                }
	
	            	he.close();
	            }
	        });
	        server.createContext(indexPath, new HttpHandler() {
	        	@Override
	            public void handle(HttpExchange he) throws IOException {
	            	String subpath = he.getRequestURI().getPath().substring(indexPath.length());
	            	int statusCode;
	            	JSONObject response;
	                if ("PUT".equalsIgnoreCase(he.getRequestMethod())) {
	                	switch (subpath) {
	                		case "":
	                            requestUri[0] = he.getRequestURI().getPath();
	                            try (InputStream in  = he.getRequestBody()) {
	                            	JSONObject mapping = new JSONObject(IOUtils.toString(in, StandardCharsets.UTF_8));
	                                JSONObject fields = mapping.getJSONObject("mappings").getJSONObject("properties");
	                                for (String field : (Set<String>) fields.keySet()) {
	                                	fieldDefns.put(field, fields.getJSONObject(field));
	                                }
	                            }
			                    statusCode = HttpURLConnection.HTTP_OK;
	                            response = new JSONObject();
	                            response.put("acknowledged", true);
	                            response.put("shards_acknowledged", true);
	                            response.put("index", indexName);
	                			break;
	                		case "/_bulk":
			                    requestUri[1] = he.getRequestURI().getPath();
			                    try (BufferedReader br = new BufferedReader(new InputStreamReader(he.getRequestBody(), StandardCharsets.UTF_8))) {
			                        String line;
			                        while ((line = br.readLine()) != null) {
			                        	if (!line.isEmpty()) {
			                        		bulkBody.add(new JSONObject(line));
			                        	}
			                        }
			                    }
			                    statusCode = HttpURLConnection.HTTP_OK;
			                    response = new JSONObject();
			                    response.put("took", 17);
			                    response.put("errors", false);
			                    response.put("items", Arrays.asList());
			                    break;
	                		default:
			                    statusCode = HttpURLConnection.HTTP_NOT_FOUND;
	                			response = null;
	                	}
	                } else if ("POST".equalsIgnoreCase(he.getRequestMethod())) {
	                	switch (subpath) {
	                		case "/_refresh":
			                    requestUri[2] = he.getRequestURI().getPath();
	                			JSONObject shards = new JSONObject();
	                			shards.put("total", 1);
	                			shards.put("successful", 1);
	                			shards.put("failed", 0);
			                    statusCode = HttpURLConnection.HTTP_OK;
	                			response = new JSONObject();
	                			response.put("_shards", shards);
	                			break;
	                		default:
			                    statusCode = HttpURLConnection.HTTP_NOT_FOUND;
	                			response = null;
	                	}
	                } else if ("HEAD".equalsIgnoreCase(he.getRequestMethod())) {
	                    statusCode = HttpURLConnection.HTTP_OK;
	                    response = null;
	                } else if ("GET".equalsIgnoreCase(he.getRequestMethod())) {
	                	switch (subpath) {
	                		case "/_search_shards":
	                			JSONObject shard = new JSONObject();
	                			shard.put("shard", 0);
	                			shard.put("state", "STARTED");
	                			shard.put("primary", true);
	                			shard.put("index", indexName);
	                			shard.put("node", NODE_ID);
			                    statusCode = HttpURLConnection.HTTP_OK;
	                			response = new JSONObject();
	                			response.put("shards", Arrays.asList(Arrays.asList(shard)));
	                			break;
	                		case "/_mapping":
			                    statusCode = HttpURLConnection.HTTP_OK;
			                    JSONObject properties = new JSONObject();
			                    for (Map.Entry<String, JSONObject> entry : fieldDefns.entrySet()) {
			                    	properties.put(entry.getKey(), entry.getValue());
			                    }
			                    JSONObject mappings = new JSONObject(); 
			                    mappings.put("properties", properties);
			                    JSONObject indexInfo = new JSONObject();
			                    indexInfo.put("mappings", mappings);
	                			response = new JSONObject();
	                			response.put(indexName, indexInfo);
			                    statusCode = HttpURLConnection.HTTP_OK;
	                			break;
	                		default:
			                    statusCode = HttpURLConnection.HTTP_NOT_FOUND;
	                			response = null;
	                	}
	                } else {
	                	statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
	                	response = null;
	                }
	
	            	if (statusCode >= 400) {
	                	he.sendResponseHeaders(statusCode, 0);
	                    try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
	                        writer.write(he.getRequestMethod()+" "+he.getRequestURI());
	                    }
	            	} else {
	                	he.sendResponseHeaders(statusCode, response != null ? 0 : -1);
		                if (response != null) {
		                    try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
		                        response.write(writer);
		                    }
		                }
	                }
	
	                he.close();
	            }
	        });
        }

        void start() {
        	server.start();
        }

        String getIndexUrl() {
            int serverPort = server.getAddress().getPort();
            String indexUrl = "http://localhost:" + serverPort + indexPath;
            return indexUrl;
        }

        void stop() {
        	server.stop(0);
        }
    }
}
