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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.common.lang.FileFormat;
import org.eclipse.rdf4j.common.lang.service.FileFormatServiceRegistry;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.eclipse.rdf4j.query.resultio.*;
import org.eclipse.rdf4j.repository.sail.*;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;

import javax.activation.MimeType;
import javax.activation.MimeTypeParseException;
import java.io.*;
import java.net.URLDecoder;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles HTTP requests containing SPARQL queries.
 *
 * <p> Supported are only SPARQL queries. SPARQL updates are <b>not</b> supported. One request <b>must</b> include
 * exactly one SPARQL query string. Supported are only HTTP GET and HTTP POST methods. For each detailed HTTP method
 * specification see <a href="https://www.w3.org/TR/sparql11-protocol/#protocol">SPARQL Protocol Operations</a></p>
 *
 * @author sykorjan
 */
public class HttpSparqlHandler implements HttpHandler {
    private static final String AND_DELIMITER = "&";
    private final String CHARSET = "UTF-8";

    // Query parameter prefixes
    private static final String QUERY_PREFIX = "query=";
    private static final String DEFAULT_GRAPH_PREFIX = "default-graph-uri=";
    private static final String NAMED_GRAPH_PREFIX = "named-graph-uri=";

    // Request content type (only for POST requests)
    private static final String ENCODED_CONTENT = "application/x-www-form-urlencoded";
    private static final String UNENCODED_CONTENT = "application/sparql-query";


    // Service successfully executes the query
    private static final int HTTP_OK_STATUS = 200;
    // Client does not provide a correct request, including incorrect request parameter content (e.g. illegal request
    // parameters or illegal sequence of query characters defined by the SPARQL query grammar)
    private static final int HTTP_BAD_REQUEST = 400;
    // Client provides a correct request but service fails to process it (e.g. server failed to send the response)
    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;

    // Connection to the Sail repository
    private final SailRepositoryConnection connection;
    // Logger
    private static final Logger LOGGER = Logger.getLogger(HttpSparqlHandler.class.getName());

    /**
     * Default HttpSparqlHandler (verbose mode disabled by default)
     *
     * @param connection connection to Sail repository
     */
    public HttpSparqlHandler(SailRepositoryConnection connection) {
        this(connection, false);
    }

    /**
     * @param connection connection to Sail repository
     * @param verbose    true when verbose mode enabled, otherwise false
     */
    public HttpSparqlHandler(SailRepositoryConnection connection, boolean verbose) {
        this.connection = connection;
        if (!verbose) {
            // Verbose mode disabled --> logs with level lower than WARNING will be discarded
            LOGGER.setLevel(Level.WARNING);
        }
    }

    /**
     * Handle HTTP requests containing SPARQL queries.
     *
     * <p>First, SPARQL query is retrieved from the HTTP request. Then the query is evaluated towards a
     * specified Sail repository. Finally, the query result is sent back as an HTTP response.</p>
     *
     * @param exchange HTTP exchange
     * @throws IOException If an error occurs during sending the error response
     */
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            SparqlQuery sparqlQuery = retrieveQuery(exchange);
            evaluateQuery(sparqlQuery, exchange);
        } catch (IllegalArgumentException | RDF4JException e) {
            StringWriter sw = new StringWriter();
            PrintWriter w = new PrintWriter(sw);
            e.printStackTrace(w);
            LOGGER.log(Level.WARNING, sw.toString());
            sendResponse(exchange, HTTP_BAD_REQUEST, sw.toString());
        } catch (IOException | RuntimeException e) {
            StringWriter sw = new StringWriter();
            PrintWriter w = new PrintWriter(sw);
            e.printStackTrace(w);
            LOGGER.log(Level.WARNING, sw.toString());
            sendResponse(exchange, HTTP_INTERNAL_SERVER_ERROR, sw.toString());
        }

    }

    /**
     * Retrieve SPARQL query from the HTTP request.
     *
     * @param exchange HTTP request
     * @return SPARQL query
     * @throws IOException              If an error occurs during reading the request
     * @throws IllegalArgumentException If the request does not follow the SPARQL Protocol Operation specification
     */
    private SparqlQuery retrieveQuery(HttpExchange exchange) throws IOException, IllegalArgumentException {
        String requestMethod = exchange.getRequestMethod();
        SparqlQuery sparqlQuery = new SparqlQuery();
        // Help variable for checking for multiple query parameters
        int queryCount = 0;
        // Factory for IRI creation
        ValueFactory factory = SimpleValueFactory.getInstance();

        if ("GET".equalsIgnoreCase(requestMethod)) {
            // Retrieve from the request URI parameter query and optional parameters defaultGraphs and namedGraphs
            String requestQuery = exchange.getRequestURI().getQuery();
            if (requestQuery != null) {
                StringTokenizer stk = new StringTokenizer(requestQuery, AND_DELIMITER);
                while (stk.hasMoreTokens()) {
                    String param = stk.nextToken();
                    if (param.startsWith(QUERY_PREFIX)) {
                        queryCount++;
                        sparqlQuery.setQuery(URLDecoder.decode(param.substring(QUERY_PREFIX.length()), CHARSET));
                    } else if (param.startsWith(DEFAULT_GRAPH_PREFIX)) {
                        sparqlQuery.addDefaultGraph(factory.createIRI(URLDecoder.decode(param.substring(
                                DEFAULT_GRAPH_PREFIX.length()), CHARSET)));
                    } else if (param.startsWith(NAMED_GRAPH_PREFIX)) {
                        sparqlQuery.addNamedGraph(factory.createIRI(URLDecoder.decode(param.substring(
                                NAMED_GRAPH_PREFIX.length()), CHARSET)));
                    } else {
                        throw new IllegalArgumentException("Unspecified request parameter: " + param);
                    }
                }
            }
            try (InputStream requestBody = exchange.getRequestBody()) {
                if (requestBody.available() > 0) {
                    throw new IllegalArgumentException("Request via GET must not include a message body");
                }
            }
        } else if ("POST".equalsIgnoreCase(requestMethod)) {
            Headers headers = exchange.getRequestHeaders();

            // Check for presence of the Content-Type header
            if (!headers.containsKey("Content-Type")) {
                throw new IllegalArgumentException("POST request has to contain header \"Content-Type\"");
            }

            // Should not happen but better be safe than sorry
            if (headers.get("Content-Type").size() != 1) {
                throw new IllegalArgumentException("POST request has to contain header \"Content-Type\" exactly once");
            }

            // Check Content-Type header content
            try {
                MimeType mimeType = new MimeType(headers.getFirst("Content-Type"));
                String baseType = mimeType.getBaseType();
                String charset = mimeType.getParameter("charset");
                if (charset != null && !charset.equals(CHARSET)) {
                    throw new IllegalArgumentException("Illegal Content-Type charset. Only UTF-8 is supported");
                }

                // Request message body is processed based on the value of Content-Type property
                if (baseType.equals(ENCODED_CONTENT)) {
                    // Retrieve from the message body parameter query and optional parameters defaultGraphs and
                    // namedGraphs
                    try (InputStream requestBody = exchange.getRequestBody()) {
                        Scanner requestBodyScanner = new Scanner(requestBody, CHARSET).useDelimiter(AND_DELIMITER);
                        while (requestBodyScanner.hasNext()) {
                            String parameter = requestBodyScanner.next();
                            if (parameter.startsWith(QUERY_PREFIX)) {
                                queryCount++;
                                sparqlQuery.setQuery(URLDecoder.decode(parameter.substring(QUERY_PREFIX.length()),
                                        CHARSET));
                            } else if (parameter.startsWith(DEFAULT_GRAPH_PREFIX)) {
                                sparqlQuery.addDefaultGraph(factory.createIRI(URLDecoder.decode(
                                        parameter.substring(DEFAULT_GRAPH_PREFIX.length()), CHARSET)));
                            } else if (parameter.startsWith(NAMED_GRAPH_PREFIX)) {
                                sparqlQuery.addNamedGraph(factory.createIRI(URLDecoder.decode(
                                        parameter.substring(NAMED_GRAPH_PREFIX.length()), CHARSET)));
                            } else {
                                throw new IllegalArgumentException("Unspecified request parameter: " + parameter);
                            }
                        }
                    }
                } else if (baseType.equals(UNENCODED_CONTENT)) {
                    // Retrieve from the request URI optional parameters defaultGraphs and namedGraphs
                    String requestQuery = exchange.getRequestURI().getQuery();
                    if (requestQuery != null) {
                        StringTokenizer stk = new StringTokenizer(requestQuery, AND_DELIMITER);
                        while (stk.hasMoreTokens()) {
                            String param = stk.nextToken();
                            if (param.startsWith(DEFAULT_GRAPH_PREFIX)) {
                                sparqlQuery.addDefaultGraph(factory.createIRI(
                                        URLDecoder.decode(param.substring(DEFAULT_GRAPH_PREFIX.length()), CHARSET)));
                            } else if (param.startsWith(NAMED_GRAPH_PREFIX)) {
                                sparqlQuery.addNamedGraph(factory.createIRI(
                                        URLDecoder.decode(param.substring(NAMED_GRAPH_PREFIX.length()), CHARSET)));
                            } else {
                                throw new IllegalArgumentException("Unspecified request parameter: " + param);
                            }
                        }
                    }

                    // Retrieve from the message body parameter query
                    try (InputStream requestBody = exchange.getRequestBody()) {
                        sparqlQuery.setQuery(new Scanner(requestBody, CHARSET).useDelimiter("\\A").next());
                    }
                } else {
                    throw new IllegalArgumentException("Content-Type of POST request has to be either " + ENCODED_CONTENT
                            + " or " + UNENCODED_CONTENT);
                }
            } catch (MimeTypeParseException e) {
                throw new IllegalArgumentException("Illegal Content-Type header content");
            }
        } else {
            throw new IllegalArgumentException("Request method has to be only either GET or POST");
        }

        if (sparqlQuery.getQuery() == null || sparqlQuery.getQuery().length() <= 0) {
            throw new IllegalArgumentException("Missing parameter query");
        }

        if (queryCount > 1) {
            throw new IllegalArgumentException("Cannot invoke query operation with more than one query string");
        }

        return sparqlQuery;
    }

    /**
     * Evaluate query towards a Sail repository and send response (the query result) back to client
     *
     * @param sparqlQuery query to be evaluated
     * @param exchange    HTTP exchange for sending the response
     * @throws IOException    If an error occurs during sending response to the client
     * @throws RDF4JException If an error occurs due to illegal SPARQL query (e.g. incorrect syntax)
     */
    private void evaluateQuery(SparqlQuery sparqlQuery, HttpExchange exchange) throws IOException, RDF4JException {
        SailQuery query = connection.prepareQuery(QueryLanguage.SPARQL, sparqlQuery.getQuery(), null);
        Dataset dataset = sparqlQuery.getDataset();
        if(!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
            // This will include default graphs and named graphs from  the request parameters but default graphs and
            // named graphs contained in the string query will be ignored
            query.getParsedQuery().setDataset(dataset);
        }
        List<String> acceptedMimeTypes = exchange.getRequestHeaders().get("Accept");
        if (query instanceof SailTupleQuery) {
            LOGGER.log(Level.INFO, "Evaluating tuple query: " + sparqlQuery.getQuery());
            QueryResultFormat format = getFormat(TupleQueryResultWriterRegistry.getInstance(), acceptedMimeTypes,
                    TupleQueryResultFormat.SPARQL, exchange.getResponseHeaders());
            exchange.sendResponseHeaders(HTTP_OK_STATUS, 0);
            try (BufferedOutputStream response = new BufferedOutputStream(exchange.getResponseBody())) {
                ((TupleQuery) query).evaluate(TupleQueryResultWriterRegistry.getInstance().get(format).get()
                        .getWriter(response));
            }
        } else if (query instanceof SailGraphQuery) {
            LOGGER.log(Level.INFO, "Evaluating graph query: " + sparqlQuery.getQuery());
            RDFFormat format = getFormat(RDFWriterRegistry.getInstance(), acceptedMimeTypes, RDFFormat.RDFXML,
                    exchange.getResponseHeaders());
            exchange.sendResponseHeaders(HTTP_OK_STATUS, 0);
            try (BufferedOutputStream response = new BufferedOutputStream(exchange.getResponseBody())) {
                ((GraphQuery) query).evaluate(RDFWriterRegistry.getInstance().get(format).get().getWriter(response));
            }
        } else if (query instanceof SailBooleanQuery) {
            LOGGER.log(Level.INFO, "Evaluating boolean query: " + sparqlQuery.getQuery());
            QueryResultFormat format = getFormat(BooleanQueryResultWriterRegistry.getInstance(),
                    acceptedMimeTypes, BooleanQueryResultFormat.SPARQL, exchange.getResponseHeaders());
            exchange.sendResponseHeaders(HTTP_OK_STATUS, 0);
            try (BufferedOutputStream response = new BufferedOutputStream(exchange.getResponseBody())) {
                BooleanQueryResultWriterRegistry.getInstance().get(format).get().getWriter(response)
                        .write(((BooleanQuery) query).evaluate());
            }
        }
        LOGGER.log(Level.INFO, "Request successfully processed");
    }

    /**
     * Send response of the processed request to the client
     *
     * @param exchange HttpExchange wrapper encapsulating response
     * @param code     HTTP code
     * @param message  Content of the response
     * @throws IOException
     */
    private void sendResponse(HttpExchange exchange, int code, String message) throws IOException {
        exchange.sendResponseHeaders(code, message.length());
        OutputStream os = exchange.getResponseBody();
        os.write(message.getBytes());
        os.close();
    }

    /**
     * Get file format of the response and set its MIME type as a response content-type header value.
     * <p>
     * If no accepted MIME types are specified, a default file format for the default MIME type is chosen.
     *
     * @param reg           requested result writer registry
     * @param mimeTypes     requested/accepted response MIME types (e.g. application/sparql-results+xml,
     *                      application/xml)
     * @param defaultFormat default file format (e.g. SPARQL/XML)
     * @param h             response headers for setting response content-type value
     * @param <FF>          file format
     * @param <S>
     * @return
     */
    private <FF extends FileFormat, S extends Object> FF getFormat(FileFormatServiceRegistry<FF, S> reg,
                                                                   List<String> mimeTypes, FF defaultFormat,
                                                                   Headers h) {
        if (mimeTypes != null) {
            for (String mimeType : mimeTypes) {
                Optional<FF> o = reg.getFileFormatForMIMEType(mimeType);
                if (o.isPresent()) {
                    h.set("Content-Type", mimeType);
                    return o.get();
                }
            }
        }
        h.set("Content-Type", defaultFormat.getDefaultMIMEType());
        return defaultFormat;
    }

    /**
     * Help class for retrieving the whole SPARQL query, including optional parameters defaultGraphs and namedGraphs,
     * from the HTTP request.
     */
    private class SparqlQuery {
        // SPARQL query string, has to be exactly one
        private String query;
        // Dataset containing default graphs and named graphs
        private SimpleDataset dataset;

        public SparqlQuery() {
            query = null;
            dataset = new SimpleDataset();
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public void addDefaultGraph(IRI defaultGraph) {
            dataset.addDefaultGraph(defaultGraph);
        }

        public void addNamedGraph(IRI namedGraph) {
            dataset.addNamedGraph(namedGraph);
        }

        public Dataset getDataset() {
            return dataset;
        }

    }

}
