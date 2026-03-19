package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.repository.HBaseRepository;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.*;

public class HttpRequestTest {
	private static final int QUERY_TIMEOUT = 15;

	private Configuration conf;

	@BeforeEach
	public void setup() throws Exception {
		conf = HBaseServerTestInstance.getInstanceConfig();
	}

	protected final Repository createRepo(String tableName) throws Exception {
		HBaseSail hbaseSail = new HBaseSail(conf, tableName, true, 0, true, QUERY_TIMEOUT, null);
		Repository hbaseRepo = new HBaseRepository(hbaseSail);
		hbaseRepo.init();
		return hbaseRepo;
	}

	@Test
	public void simpleRequestTest() throws Exception {
		String expectedBody = "Hello world!";
		try (MockHttpServer server = startHttpServer("text/plain", toBytes(expectedBody))) {
			Repository hbaseRepo = createRepo("testSimpleRequest");
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				TupleQuery q = conn.prepareTupleQuery(
						"PREFIX halyard: <http://merck.github.io/Halyard/ns#> PREFIX http: <http://www.w3.org/2011/http#> select * { [] a http:Request; http:absoluteURI '" + server.getUrl()
								+ "'; http:resp [http:statusCodeValue ?sc; http:reasonPhrase ?reason; http:body ?body ] }");
				try (TupleQueryResult iter = q.evaluate()) {
					assertTrue(iter.hasNext());
					BindingSet bs = iter.next();
					assertEquals(200, ((Literal) bs.getValue("sc")).intValue());
					assertEquals(expectedBody, ((Literal) bs.getValue("body")).stringValue());
					assertFalse(iter.hasNext());
				}
			}
			hbaseRepo.shutDown();
		}
	}

	@Test
	public void headerRequestTest() throws Exception {
		String userAgent = "foobar";
		String expectedBody = "{\"msg\":\"Hi!\"}";
		try (MockHttpServer server = startHttpServer("application/json", toBytes(expectedBody))) {
			Repository hbaseRepo = createRepo("testSimpleRequest");
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				TupleQuery q = conn.prepareTupleQuery("PREFIX halyard: <http://merck.github.io/Halyard/ns#> PREFIX http: <http://www.w3.org/2011/http#> select * { [] a http:Request; http:absoluteURI '" + server.getUrl()
						+ "'; http:headers ([http:fieldName 'User-agent'; http:fieldValue '" + userAgent + "']); http:resp [http:statusCodeValue ?sc; http:reasonPhrase ?reason; http:body ?body ] }");
				try (TupleQueryResult iter = q.evaluate()) {
					assertTrue(iter.hasNext());
					BindingSet bs = iter.next();
					assertNotNull(server.requestHeaders);
					assertEquals(userAgent, server.requestHeaders.get("User-agent").get(0));
					assertEquals(200, ((Literal) bs.getValue("sc")).intValue());
					Literal actualBody = (Literal) bs.getValue("body");
					assertEquals(expectedBody, actualBody.stringValue());
					assertEquals(HALYARD.MAP_TYPE, actualBody.getDatatype());
					assertFalse(iter.hasNext());
				}
			}
			hbaseRepo.shutDown();
		}
	}

	@Test
	public void postRequestTest() throws Exception {
		String contentType = "text/plain";
		String requestBody = "foobar";
		String expectedBody = "{\"msg\":\"Hi!\"}";
		try (MockHttpServer server = startHttpServer("application/json", toBytes(expectedBody))) {
			Repository hbaseRepo = createRepo("testSimpleRequest");
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				TupleQuery q = conn.prepareTupleQuery("PREFIX halyard: <http://merck.github.io/Halyard/ns#> PREFIX http: <http://www.w3.org/2011/http#> select * { [] a http:Request; http:absoluteURI '" + server.getUrl()
						+ "'; http:headers ([http:fieldName 'Content-type'; http:fieldValue '" + contentType + "']); http:body '" + requestBody + "'; http:resp [http:statusCodeValue ?sc; http:reasonPhrase ?reason; http:body ?body ] }");
				try (TupleQueryResult iter = q.evaluate()) {
					assertTrue(iter.hasNext());
					BindingSet bs = iter.next();
					assertNotNull(server.requestHeaders);
					assertEquals(contentType, server.requestHeaders.get("Content-type").get(0));
					assertNotNull(server.requestBody);
					assertEquals(requestBody, server.requestBody);
					assertEquals(200, ((Literal) bs.getValue("sc")).intValue());
					Literal actualBody = (Literal) bs.getValue("body");
					assertEquals(expectedBody, actualBody.stringValue());
					assertEquals(HALYARD.MAP_TYPE, actualBody.getDatatype());
					assertFalse(iter.hasNext());
				}
			}
			hbaseRepo.shutDown();
		}
	}

	private MockHttpServer startHttpServer(String contentType, byte[] response) throws IOException {
		MockHttpServer server = new MockHttpServer(contentType, response);
		server.start();
		return server;
	}

	private static byte[] toBytes(String s) {
		ByteBuffer bb = StandardCharsets.UTF_8.encode(s);
		byte[] b = new byte[bb.remaining()];
		bb.get(b);
		return b;
	}

	static class MockHttpServer implements AutoCloseable {
		final HttpServer server;
		Headers requestHeaders;
		String requestBody;

		MockHttpServer(String contentType, byte[] response) throws IOException {
			server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
			server.createContext("/", new HttpHandler() {
				@Override
				public void handle(HttpExchange he) throws IOException {
					requestHeaders = he.getRequestHeaders();
					try (InputStream in = he.getRequestBody()) {
						requestBody = IOUtils.toString(in, StandardCharsets.UTF_8);
					}
					he.getResponseHeaders().add("Content-type", contentType);
					he.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
					try (OutputStream out = he.getResponseBody()) {
						out.write(response);
					}
				}
			});
		}

		void start() {
			server.start();
		}

		int getPort() {
			return server.getAddress().getPort();
		}

		String getUrl() {
			String indexUrl = "http://localhost:" + getPort();
			return indexUrl;
		}

		public void close() {
			server.stop(0);
		}
	}
}

