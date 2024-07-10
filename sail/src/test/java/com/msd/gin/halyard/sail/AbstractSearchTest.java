package com.msd.gin.halyard.sail;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.repository.HBaseRepository;
import com.msd.gin.halyard.sail.search.SearchDocument;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.repository.Repository;
import org.json.JSONObject;
import org.junit.Before;

public abstract class AbstractSearchTest {
	private static final int QUERY_TIMEOUT = 15;

	protected static final String INDEX_NAME = "myIndex";
	protected Configuration conf;
	private RDFFactory rdfFactory;

	@Before
	public void setup() throws Exception {
		conf = HBaseServerTestInstance.getInstanceConfig();
		rdfFactory = RDFFactory.create(conf);
	}

	protected final Repository createRepo(String tableName, MockElasticServer esServer) throws Exception {
		HBaseSail hbaseSail = new HBaseSail(conf, tableName, true, 0, true, QUERY_TIMEOUT, ElasticSettings.from(new URI(esServer.getIndexUrl()).toURL()), null);
		Repository hbaseRepo = new HBaseRepository(hbaseSail);
		hbaseRepo.init();
		return hbaseRepo;
	}

	protected final MockElasticServer startElasticsearch(String expectedRequest, Literal... response) throws IOException, InterruptedException {
		return startElasticsearch(Collections.singletonMap(expectedRequest, response));
	}

	protected final MockElasticServer startElasticsearch(List<Pair<String, Literal[]>> reqRespPairs) throws IOException, InterruptedException {
		Map<String, Literal[]> requestResponses = new HashMap<>();
		for (Pair<String, Literal[]> reqResp : reqRespPairs) {
			requestResponses.put(reqResp.getKey(), reqResp.getValue());
		}
		return startElasticsearch(requestResponses);
	}

	protected final MockElasticServer startElasticsearch(Map<String, Literal[]> requestResponses) throws IOException, InterruptedException {
		MockElasticServer server = new MockElasticServer(INDEX_NAME, requestResponses, rdfFactory);
		server.start();
		return server;
	}

	static class MockElasticServer implements AutoCloseable {
		private static final String ES_VERSION = "8.4.2";
		private static final String NODE_ID = UUID.randomUUID().toString();

		final String indexName;
		final String indexPath;
		final HttpServer server;
		final Map<String, Literal[]> requestResponses;
		final RDFFactory rdfFactory;

		MockElasticServer(String indexName, Map<String, Literal[]> requestResponses, RDFFactory rdfFactory) throws IOException {
			this.indexName = indexName;
			this.requestResponses = requestResponses;
			this.rdfFactory = rdfFactory;
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
								httpNode.put("publish_address", server.getAddress().getHostString());
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
							writer.write(he.getRequestMethod() + " " + he.getRequestURI());
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
					int statusCode;
					String response;
					if ("POST".equalsIgnoreCase(he.getRequestMethod())) {
						String requestBody;
						try (InputStream in = he.getRequestBody()) {
							requestBody = IOUtils.toString(in, StandardCharsets.UTF_8);
						}
						Literal[] responseValues = requestResponses.get(requestBody);
						if (responseValues != null) {
							statusCode = HttpURLConnection.HTTP_OK;
							response = createResponse(responseValues);
						} else {
							throw new AssertionError("Unexpected request: " + requestBody);
						}
					} else {
						statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
						response = null;
					}

					if (statusCode >= 400) {
						he.sendResponseHeaders(statusCode, 0);
						try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
							writer.write(he.getRequestMethod() + " " + he.getRequestURI());
						}
					} else {
						he.getResponseHeaders().set("Content-type", "application/json; charset=UTF-8");
						he.getResponseHeaders().set("X-Elastic-Product", "Elasticsearch");
						he.sendResponseHeaders(statusCode, response != null ? 0 : -1);
						if (response != null) {
							try (OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), StandardCharsets.UTF_8)) {
								writer.write(response);
							}
						}
					}

					he.close();
				}
			});
		}

		private String createResponse(Literal[] values) throws IOException {
			StringWriter jsonBuf = new StringWriter();
			JsonGenerator jsonGen = new JsonFactory().createGenerator(jsonBuf);
			jsonGen.writeStartObject();
			jsonGen.writeNumberField("took", 34);
			jsonGen.writeBooleanField("timed_out", false);
			jsonGen.writeObjectFieldStart("_shards");
			jsonGen.writeNumberField("total", 5);
			jsonGen.writeNumberField("successful", 5);
			jsonGen.writeNumberField("skipped", 0);
			jsonGen.writeNumberField("failed", 0);
			jsonGen.writeEndObject();
			jsonGen.writeObjectFieldStart("hits");
			jsonGen.writeArrayFieldStart("hits");
			for (int i = 0; i < values.length; i++) {
				Literal val = values[i];
				jsonGen.writeStartObject();
				jsonGen.writeStringField("_index", indexName);
				String id = rdfFactory.id(val).toString();
				jsonGen.writeStringField("_id", id);
				jsonGen.writeNumberField("_score", (double) (values.length - i));
				jsonGen.writeObjectFieldStart("_source");
				jsonGen.writeStringField(SearchDocument.ID_FIELD, id);
				jsonGen.writeStringField(SearchDocument.LABEL_FIELD, val.getLabel());
				jsonGen.writeStringField(SearchDocument.DATATYPE_FIELD, val.getDatatype().stringValue());
				if (val.getLanguage().isPresent()) {
					jsonGen.writeStringField(SearchDocument.LANG_FIELD, val.getLanguage().get());
				}
				jsonGen.writeEndObject();
				jsonGen.writeEndObject();
			}
			jsonGen.writeEndArray();
			jsonGen.writeEndObject();
			jsonGen.writeEndObject();
			jsonGen.close();
			return jsonBuf.toString();
		}

		void start() {
			server.start();
		}

		int getPort() {
			return server.getAddress().getPort();
		}

		String getIndexUrl() {
			String indexUrl = "http://localhost:" + getPort() + indexPath;
			return indexUrl;
		}

		public void close() {
			server.stop(0);
		}
	}
}
