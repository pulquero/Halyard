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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msd.gin.halyard.repository.HBaseRepository;
import com.msd.gin.halyard.sail.ElasticSettings;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;
import com.msd.gin.halyard.sail.ResultTrackingSailConnection;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.activation.MimeType;
import javax.activation.MimeTypeParseException;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.common.lang.FileFormat;
import org.eclipse.rdf4j.common.lang.service.FileFormatServiceRegistry;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.AbstractTupleQueryResultHandler;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.BooleanQueryResultHandler;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Operation;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResultHandler;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.algebra.Clear;
import org.eclipse.rdf4j.query.algebra.Modify;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriterFactory;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriterRegistry;
import org.eclipse.rdf4j.query.resultio.QueryResultFormat;
import org.eclipse.rdf4j.query.resultio.QueryResultIO;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParser;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParserRegistry;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterFactory;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.repository.sail.SailQuery;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailUpdate;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParserRegistry;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles HTTP requests containing SPARQL queries.
 *
 * <p> Supported are only SPARQL queries. One request <b>must</b> include
 * exactly one SPARQL query string. Supported are only HTTP GET and HTTP POST methods. For each detailed HTTP method
 * specification see <a href="https://www.w3.org/TR/sparql11-protocol/#protocol">SPARQL Protocol Operations</a></p>
 *
 * @author sykorjan
 */
public final class HttpSparqlHandler implements HttpHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSparqlHandler.class);

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final Pattern UNRESOLVED_PARAMETERS = Pattern.compile("\\{\\{(\\w+)\\}\\}");

    // Query parameter prefixes
    private static final String QUERY_PARAM = "query";
    private static final String DEFAULT_GRAPH_PARAM = "default-graph-uri";
    private static final String NAMED_GRAPH_PARAM = "named-graph-uri";
    private static final String UPDATE_PARAM = "update";
    private static final String USING_GRAPH_URI_PARAM = "using-graph-uri";
    private static final String USING_NAMED_GRAPH_PARAM = "using-named-graph-uri";
    private static final String DEFAULT_PARAM = "default";
    private static final String GRAPH_PARAM = "graph";

    private static final String TRACK_RESULT_SIZE_PARAM = "track-result-size";
    private static final String MAP_REDUCE_PARAM = "map-reduce";
    private static final String TARGET_PARAM = "target";

    static final String JSON_CONTENT = "application/json";
    // Request content type (only for POST requests)
    static final String ENCODED_CONTENT = "application/x-www-form-urlencoded";
    static final String UNENCODED_QUERY_CONTENT = "application/sparql-query";
    static final String UNENCODED_UPDATE_CONTENT = "application/sparql-update";

    static final String JMX_ENDPOINT = "/_management/";
    static final String HEALTH_ENDPOINT = "/_health";
    static final String STOP_ENDPOINT = "/_stop";

    private final SailRepository repository;
    private final Properties storedQueries;
    private final WriterConfig writerConfig;
    private final Runnable stopAction;
    private final String serviceDescriptionQuery;

    /**
     * @param rep              Sail repository
     * @param storedQueries    pre-defined stored SPARQL query templates
     * @param writerProperties RDF4J RIO WriterConfig properties
     */
    @SuppressWarnings("unchecked")
    public HttpSparqlHandler(SailRepository rep, Properties storedQueries, Properties writerProperties, Runnable stopAction) {
        this.repository = rep;
        this.storedQueries = storedQueries;
        this.writerConfig = new WriterConfig();
        if (writerProperties != null) {
            for (Map.Entry<Object, Object> me : writerProperties.entrySet()) {
                writerConfig.set((RioSetting<Object>) getStaticField(me.getKey().toString()), getStaticField(me.getValue().toString()));
            }
        }
        this.stopAction = stopAction;
        this.serviceDescriptionQuery = getServiceDescriptionQuery();
    }

    private String getServiceDescriptionQuery() {
        List<String> inputFormats = new ArrayList<>();
        for (RDFFormat rdfFormat : RDFParserRegistry.getInstance().getKeys()) {
        	if (rdfFormat.hasStandardURI()) {
        		inputFormats.add(NTriplesUtil.toNTriplesString(rdfFormat.getStandardURI()));
        	}
        }
        List<String> outputFormats = new ArrayList<>();
        for (RDFFormat rdfFormat : RDFWriterRegistry.getInstance().getKeys()) {
        	if (rdfFormat.hasStandardURI()) {
        		outputFormats.add(NTriplesUtil.toNTriplesString(rdfFormat.getStandardURI()));
        	}
        }
        for (QueryResultFormat qrFormat : TupleQueryResultWriterRegistry.getInstance().getKeys()) {
        	if (qrFormat.hasStandardURI()) {
        		outputFormats.add(NTriplesUtil.toNTriplesString(qrFormat.getStandardURI()));
        	}
        }
        for (QueryResultFormat qrFormat : BooleanQueryResultWriterRegistry.getInstance().getKeys()) {
        	if (qrFormat.hasStandardURI()) {
        		outputFormats.add(NTriplesUtil.toNTriplesString(qrFormat.getStandardURI()));
        	}
        }
        return
        		"PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>"
        		+ " PREFIX spin: <http://spinrdf.org/spin#>"
        		+ " PREFIX halyard: <http://merck.github.io/Halyard/ns#>"
        		+ " CONSTRUCT {"
        		+ " ?sd a sd:Service; "
        		+ " sd:endpoint ?endpoint; "
        		+ " sd:extensionFunction ?func; "
        		+ " sd:extensionAggregate ?aggr; "
        		+ " sd:propertyFeature ?magicProp; "
        		+ " sd:inputFormat ?inputFormat; "
        		+ " sd:resultFormat ?resultFormat; "
        		+ " sd:propertyFeature ?magicProp; "
        		+ " sd:defaultDataset halyard:statsContext. "
        		+ " ?func a sd:Function. "
        		+ " ?aggr a sd:Aggregate. "
        		+ " ?s ?p ?o"
        		+ " }"
        		+ " WHERE {"
        		+ " {"
        		+ " GRAPH halyard:statsContext {?s ?p ?o}"
        		+ " } UNION {"
        		+ " GRAPH halyard:functions {?func a sd:Function FILTER NOT EXISTS {?func rdfs:subClassOf <builtin:Functions>} }"
        		+ " } UNION {"
        		+ " GRAPH halyard:functions {?aggr a sd:Aggregate}"
        		+ " } UNION {"
        		+ " GRAPH halyard:functions {?magicProp a spin:MagicProperty}"
        		+ " } UNION {"
        		+ " VALUES ?inputFormat {" + String.join(" ", inputFormats) + "}"
        		+ " } UNION {"
        		+ " VALUES ?outputFormat {" + String.join(" ", outputFormats) + "}"
        		+ " }"
        		+ " }";
    }

    /**
     * retrieves public static field using reflection
     *
     * @param fqn fully qualified class.field name
     * @return content of the field
     * @throws IllegalArgumentException in case of any reflection errors, class not found, field not found, illegal access...
     */
    private static Object getStaticField(String fqn) {
        try {
            int i = fqn.lastIndexOf('.');
            if (i < 1) throw new IllegalArgumentException("Invalid fully qualified name of a config field: " + fqn);
            return Class.forName(fqn.substring(0, i)).getField(fqn.substring(i + 1)).get(null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception while looking for public static field: " + fqn, e);
        }
    }

    private static boolean isUpdate(String operation) {
		String strippedOperation = QueryParserUtil.removeSPARQLQueryProlog(operation).toUpperCase();

		if (strippedOperation.startsWith("SELECT") || strippedOperation.startsWith("CONSTRUCT")
			|| strippedOperation.startsWith("DESCRIBE") || strippedOperation.startsWith("ASK")) {
			return false;
		} else {
			return true;
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
        String path = exchange.getRequestURI().getPath();
        boolean doStop = false;
        try {
        	// NB: URLs should start with an underscore to avoid collisions with stored queries
        	if (path.startsWith(JMX_ENDPOINT)) {
        		sendManagementData(exchange);
        	} else if (HEALTH_ENDPOINT.equals(path)) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
        	} else if (STOP_ENDPOINT.equals(path) && exchange.getLocalAddress().getAddress().isLoopbackAddress()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
                doStop = true;
        	} else if ("DELETE".equalsIgnoreCase(exchange.getRequestMethod())) {
        		SparqlQuery sparqlQuery = new SparqlQuery();
            	for (NameValuePair nvp : getQueryParams(exchange)) {
                    parseGraphStoreParameter(nvp, sparqlQuery);
                }
        		clearData(sparqlQuery, exchange);
        	} else {
	            SparqlQuery sparqlQuery = retrieveQuery(exchange);
	            if (sparqlQuery.getFlatFileFormat() != null) {
	            	if (sparqlQuery.getQuery() != null) {
	            		executeQueryTemplate(sparqlQuery, exchange);
	            	} else if (sparqlQuery.getUpdate() != null) {
	            		executeUpdateTemplate(sparqlQuery, exchange);
	            	}
        		} else if (sparqlQuery.getGraphFormat() != null) {
        			loadData(sparqlQuery, exchange);
        		} else if (sparqlQuery.getQuery() != null) {
        			evaluateQuery(sparqlQuery, exchange);
        		} else if (sparqlQuery.getUpdate() != null) {
        			evaluateUpdate(sparqlQuery, exchange);
        		}
        	}
        } catch (InvalidRequestException | MalformedQueryException | RDFParseException e) {
            LOGGER.debug("Bad request", e);
            sendErrorResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, e);
        } catch (Exception e) {
            LOGGER.warn("Internal error", e);
            sendErrorResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, e);
        }
        exchange.close();
        if (doStop) {
        	stopAction.run();
        }
    }

    /**
     * Retrieve SPARQL query from the HTTP request.
     *
     * @param exchange HTTP request
     * @return SPARQL query
     * @throws IOException              If an error occurs during reading the request
     * @throws InvalidRequestException If the request does not follow the SPARQL Protocol Operation specification
     */
    private SparqlQuery retrieveQuery(HttpExchange exchange) throws IOException, InvalidRequestException {
        String requestMethod = exchange.getRequestMethod();
        SparqlQuery sparqlQuery = new SparqlQuery();
        // Help variable for checking for multiple query parameters
        String path = exchange.getRequestURI().getPath();
        // retrieve query from stored queries based on non-root request URL path
        if (path != null && path.length() > 1) {
            String query = storedQueries.getProperty(path.substring(1));
            if (query == null) {
                //try to cut the extension
                int i = path.lastIndexOf('.');
                if (i > 0) {
                    query = storedQueries.getProperty(path.substring(1, i));
                }
            }
            if (query == null) {
                throw new InvalidRequestException("No stored query for path: " + path);
            }
            if (isUpdate(query)) {
            	sparqlQuery.setUpdate(query);
            } else {
            	sparqlQuery.setQuery(query);
            }
        }
        if ("GET".equalsIgnoreCase(requestMethod)) {
            try (InputStream requestBody = exchange.getRequestBody()) {
                if (requestBody.available() > 0) {
                    throw new InvalidRequestException("Request via GET must not include a message body");
                }
            }

            String requestQueryRaw = exchange.getRequestURI().getRawQuery();
            if (requestQueryRaw != null) {
            	for (NameValuePair nvp : getQueryParams(exchange)) {
                    parseQueryParameter(nvp, sparqlQuery);
                }

                String query = sparqlQuery.getQuery();
                if (query == null) {
                    throw new InvalidRequestException("Missing parameter: query");
                }
            } else if (path == null || "/".equals(path)) {
            	// service description
            	sparqlQuery.setQuery(serviceDescriptionQuery);
            	ValueFactory vf = repository.getValueFactory();
            	sparqlQuery.addBinding("sd", vf.createBNode());
            	InetSocketAddress serverAddr = exchange.getLocalAddress();
            	String server = serverAddr.getHostName();
            	int port = serverAddr.getPort();
            	sparqlQuery.addBinding("endpoint", vf.createIRI("http://" + server + ":" + port + exchange.getRequestURI().toString()));
            }
        } else if ("POST".equalsIgnoreCase(requestMethod)) {
            Headers headers = exchange.getRequestHeaders();

            // Check for presence of the Content-Type header
            if (!headers.containsKey("Content-Type")) {
                throw new InvalidRequestException("POST request has to contain header \"Content-Type\"");
            }

            // Should not happen but better be safe than sorry
            if (headers.get("Content-Type").size() != 1) {
                throw new InvalidRequestException("POST request has to contain header \"Content-Type\" exactly once");
            }

            // Check Content-Type header content
            try {
                MimeType mimeType = new MimeType(headers.getFirst("Content-Type"));
                String baseType = mimeType.getBaseType();
                String charset = mimeType.getParameter("charset");
                if (charset != null && !charset.equalsIgnoreCase(CHARSET.name())) {
                    throw new InvalidRequestException("Illegal Content-Type charset. Only UTF-8 is supported");
                }

                // Request message body is processed based on the value of Content-Type property
                if (baseType.equals(ENCODED_CONTENT)) {
                    // Retrieve from the message body parameter query and optional parameters defaultGraphs and
                    // namedGraphs
                	List<NameValuePair> queryParams = URLEncodedUtils.parse(IOUtils.toString(exchange.getRequestBody(), CHARSET), CHARSET);
                	for (NameValuePair nvp : queryParams) {
                        parseQueryParameter(nvp, sparqlQuery);
                        parseUpdateParameter(nvp, sparqlQuery);
                    }

                 	if (sparqlQuery.getQuery() == null && sparqlQuery.getUpdate() == null) {
                        throw new InvalidRequestException("Missing parameter: query/update");
                    }
                } else if (baseType.equals(UNENCODED_QUERY_CONTENT)) {
                    // Retrieve from the message body parameter query
                    try (Scanner requestBodyScanner = new Scanner(exchange.getRequestBody(), CHARSET).useDelimiter("\\A")) {
                        sparqlQuery.setQuery(requestBodyScanner.next());
                    }
                	for (NameValuePair nvp : getQueryParams(exchange)) {
                        parseQueryParameter(nvp, sparqlQuery);
                    }
                } else if (baseType.equals(UNENCODED_UPDATE_CONTENT)) {
                    // Retrieve from the message body parameter query
                    try (Scanner requestBodyScanner = new Scanner(exchange.getRequestBody(), CHARSET).useDelimiter("\\A")) {
                        sparqlQuery.setUpdate(requestBodyScanner.next());
                    }
                	for (NameValuePair nvp : getQueryParams(exchange)) {
                        parseUpdateParameter(nvp, sparqlQuery);
                    }
                } else {
                	for (NameValuePair nvp : getQueryParams(exchange)) {
                        parseQueryParameter(nvp, sparqlQuery);
                        parseUpdateParameter(nvp, sparqlQuery);
                        parseGraphStoreParameter(nvp, sparqlQuery);
                    }
                	RDFFormat rdfFormat = Rio.getParserFormatForMIMEType(baseType).orElse(null);
                	QueryResultFormat qrFormat = (rdfFormat == null) ? QueryResultIO.getParserFormatForMIMEType(baseType).orElse(null) : null;
                	if (rdfFormat != null) {
                		if (sparqlQuery.getQuery() != null || sparqlQuery.getUpdate() != null) {
                			throw new InvalidRequestException("Unexpected query for RDF data");
                		}
                		sparqlQuery.setGraphFormat(rdfFormat);
                	} else if (qrFormat != null) {
                 		if (sparqlQuery.getQuery() == null && sparqlQuery.getUpdate() == null) {
                			throw new InvalidRequestException("Missing query for flat file data");
                		}
                		sparqlQuery.setFlatFileFormat(qrFormat);
                	} else {
                		List<String> supportedTypes = new ArrayList<>();
                		supportedTypes.add(ENCODED_CONTENT);
                		supportedTypes.add(UNENCODED_QUERY_CONTENT);
                		supportedTypes.add(UNENCODED_UPDATE_CONTENT);
                		RDFParserRegistry.getInstance().getKeys().stream().map(RDFFormat::getMIMETypes).flatMap(List::stream).forEach(supportedTypes::add);
                		TupleQueryResultParserRegistry.getInstance().getKeys().stream().map(QueryResultFormat::getMIMETypes).flatMap(List::stream).forEach(supportedTypes::add);
                		throw new InvalidRequestException("Content-Type of POST request has to be one of " + supportedTypes);
                	}
                }
            } catch (MimeTypeParseException e) {
                throw new InvalidRequestException("Illegal Content-Type header content");
            }
        } else {
            throw new InvalidRequestException("Request method has to be only either GET or POST");
        }

        String query = sparqlQuery.getQuery();
        if (query != null) {
            Matcher m = UNRESOLVED_PARAMETERS.matcher(query);
            if (m.find()) {
                throw new InvalidRequestException("Missing query parameter: " + m.group(1));
            }
        }

        String update = sparqlQuery.getUpdate();
        if (update != null) {
            Matcher m = UNRESOLVED_PARAMETERS.matcher(update);
            if (m.find()) {
                throw new InvalidRequestException("Missing update parameter: " + m.group(1));
            }
        }

        return sparqlQuery;
    }

    /**
     * Parse single parameter from HTTP request parameters or body.
     *
     * @param param       single query parameter
     * @param sparqlQuery SparqlQuery to fill from the parsed parameter
     * @throws UnsupportedEncodingException which never happens
     */
    private void parseQueryParameter(NameValuePair param, SparqlQuery sparqlQuery) throws UnsupportedEncodingException, InvalidRequestException {
    	String name = param.getName();
    	String value = param.getValue();
        if (QUERY_PARAM.equals(name)) {
            sparqlQuery.setQuery(value);
        } else if (DEFAULT_GRAPH_PARAM.equals(name)) {
            sparqlQuery.addDefaultGraph(repository.getValueFactory().createIRI(value));
        } else if (NAMED_GRAPH_PARAM.equals(name)) {
            sparqlQuery.addNamedGraph(repository.getValueFactory().createIRI(value));
        } else if (TRACK_RESULT_SIZE_PARAM.equals(name)) {
        	sparqlQuery.trackResultSize = Boolean.valueOf(value);
        } else if (TARGET_PARAM.equals(name)) {
        	sparqlQuery.target = value;
        } else {
            if (name.startsWith("$")) {
            	sparqlQuery.addBinding(name.substring(1), NTriplesUtil.parseValue(value, repository.getValueFactory()));
            } else {
                sparqlQuery.addParameter(name, value);
            }
        }
    }

    private void parseUpdateParameter(NameValuePair param, SparqlQuery sparqlQuery) throws UnsupportedEncodingException, InvalidRequestException {
    	String name = param.getName();
    	String value = param.getValue();
        if (UPDATE_PARAM.equals(name)) {
            sparqlQuery.setUpdate(value);
        } else if (USING_GRAPH_URI_PARAM.equals(name)) {
            sparqlQuery.addDefaultGraph(repository.getValueFactory().createIRI(value));
        } else if (USING_NAMED_GRAPH_PARAM.equals(name)) {
            sparqlQuery.addNamedGraph(repository.getValueFactory().createIRI(value));
        } else if (TRACK_RESULT_SIZE_PARAM.equals(name)) {
        	sparqlQuery.trackResultSize = Boolean.valueOf(value);
        } else if (MAP_REDUCE_PARAM.equals(name)) {
        	sparqlQuery.mapReduce = Boolean.valueOf(value);
        } else {
            if (name.startsWith("$")) {
            	sparqlQuery.addBinding(name.substring(1), NTriplesUtil.parseValue(value, repository.getValueFactory()));
            } else {
                sparqlQuery.addParameter(name, value);
            }
        }
    }

    private void parseGraphStoreParameter(NameValuePair param, SparqlQuery sparqlQuery) throws UnsupportedEncodingException, InvalidRequestException {
    	String name = param.getName();
    	String value = param.getValue();
    	if (DEFAULT_PARAM.equals(name)) {
    		sparqlQuery.setDefaultGraph(true);
    	} else if (GRAPH_PARAM.equals(name)) {
            sparqlQuery.setGraph(repository.getValueFactory().createIRI(value));
        }
    }

    /**
     * Evaluate query towards a Sail repository and send response (the query result) back to client
     *
     * @param sparqlQuery query to be evaluated
     * @param exchange    HTTP exchange for sending the response
     * @throws IOException    If an error occurs during sending response to the client
     * @throws RDF4JException If an error occurs due to illegal SPARQL query (e.g. incorrect syntax)
     * @throws GeneralSecurityException 
     */
    private void evaluateQuery(SparqlQuery sparqlQuery, HttpExchange exchange) throws Exception {
    	if (sparqlQuery.target != null) {
    		if (sparqlQuery.mapReduce) {
                validateMapReduce(sparqlQuery);
        		HBaseSail sail = (HBaseSail) ((HBaseRepository)repository).getSail();
        		List<HalyardBulkExport.JsonInfo> infos = HalyardBulkExport.executeExport(sail.getConfiguration(), sail.getTableName(), sparqlQuery.getQuery(), sparqlQuery.target, sparqlQuery.bindings);
        		sendMapReduceResults(exchange, infos);
    		} else {
    			List<JsonExportInfo> infos = executeExport(sparqlQuery);
    			sendResults(exchange, infos);
    		}
    	} else {
    		QueryEvaluator<?,?,?> evaluator = getQueryEvaluator(sparqlQuery.getQuery(), exchange);
            OutputStream response;
            try(SailRepositoryConnection connection = repository.getConnection()) {
            	connection.begin();
    	        SailQuery query = connection.prepareQuery(QueryLanguage.SPARQL, sparqlQuery.getQuery(), null);
    	        addBindings(query, sparqlQuery.getBindings());
    	        Dataset dataset = sparqlQuery.getDataset();
    	        if (!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
    	            // This will include default graphs and named graphs from  the request parameters but default graphs and
    	            // named graphs contained in the string query will be ignored
    	            query.getParsedQuery().setDataset(dataset);
    	        }

    	        evaluator.setContentType(exchange);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                response = new BufferedOutputStream(exchange.getResponseBody());
	            LOGGER.info("Evaluating query:\nBindings: {}\n{}", sparqlQuery.getBindings(), sparqlQuery.getQuery());
    	        evaluator.evaluate(query, response);
    	        connection.commit();
        	}
            // commit the response *after* closing the connection
            response.close();
    	}
       	LOGGER.info("Query successfully processed");
    }

    private List<JsonExportInfo> executeExport(SparqlQuery sparqlQuery) throws IOException, InvalidRequestException {
		HBaseSail sail = (HBaseSail) ((HBaseRepository)repository).getSail();
        try (SailRepositoryConnection connection = repository.getConnection()) {
        	connection.begin();
	        SailQuery query = connection.prepareQuery(QueryLanguage.SPARQL, sparqlQuery.getQuery(), null);
			if (!(query instanceof TupleQuery)) {
				throw new InvalidRequestException("Only SELECT queries are supported for Elasticsearch export");
			}
	        addBindings(query, sparqlQuery.getBindings());
	        Dataset dataset = sparqlQuery.getDataset();
	        if (!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
	            // This will include default graphs and named graphs from  the request parameters but default graphs and
	            // named graphs contained in the string query will be ignored
	            query.getParsedQuery().setDataset(dataset);
	        }
            LOGGER.info("Indexing results from query:\nBindings: {}\n{}", sparqlQuery.getBindings(), sparqlQuery.getQuery());
    		try (HalyardExport.ElasticsearchWriter writer = new HalyardExport.ElasticsearchWriter(new HalyardExport.StatusLog() {
					public void tick() {}
					public void logStatus(String msg) {
						LOGGER.info(msg);
					}
				}, ElasticSettings.from(sparqlQuery.target, sail.getConfiguration()), sail.getRDFFactory(), repository.getValueFactory())) {
            	writer.writeTupleQueryResult(((TupleQuery)query).evaluate());
            	connection.commit();
            	JsonExportInfo info = new JsonExportInfo();
            	info.totalExported = writer.getExportedCount();
            	return Collections.singletonList(info);
            } catch (GeneralSecurityException e) {
            	throw new IOException(e);
			}
    	}
    }

    private QueryEvaluator<?,?,?> getQueryEvaluator(String queryString, HttpExchange exchange) throws IOException {
        // sniff query type and perform content negotiation before opening a connection
        ParsedQuery sniffedQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, queryString, null);
        if (sniffedQuery instanceof ParsedTupleQuery) {
            QueryEvaluator<QueryResultFormat, TupleQueryResultWriterFactory, TupleQueryResultHandler> evaluator = new QueryEvaluator<>(TupleQueryResultWriterRegistry.getInstance(), TupleQueryResultFormat.CSV) {
            	@Override
            	TupleQueryResultHandler getWriter(OutputStream out) {
	                TupleQueryResultWriter w = writerFactory.getWriter(out);
	                w.setWriterConfig(writerConfig);
	                return w;
            	}
            	@Override
            	void evaluate(SailQuery query, TupleQueryResultHandler handler) {
	                ((TupleQuery) query).evaluate(handler);
            	}
            };
            return evaluator;
        } else if (sniffedQuery instanceof ParsedGraphQuery) {
        	QueryEvaluator<RDFFormat, RDFWriterFactory, RDFHandler> evaluator = new QueryEvaluator<>(RDFWriterRegistry.getInstance(), RDFFormat.TURTLE) {
            	@Override
            	RDFHandler getWriter(OutputStream out) {
	                RDFWriter w = writerFactory.getWriter(out);
	                w.setWriterConfig(writerConfig);
	                return w;
            	}
            	@Override
            	void evaluate(SailQuery query, RDFHandler handler) {
	                ((GraphQuery) query).evaluate(handler);
            	}
            };
            return evaluator;
        } else if (sniffedQuery instanceof ParsedBooleanQuery) {
        	QueryEvaluator<QueryResultFormat, BooleanQueryResultWriterFactory, BooleanQueryResultHandler> evaluator = new QueryEvaluator<>(BooleanQueryResultWriterRegistry.getInstance(), BooleanQueryResultFormat.JSON) {
            	@Override
            	BooleanQueryResultHandler getWriter(OutputStream out) {
            		BooleanQueryResultWriter w = writerFactory.getWriter(out);
	                w.setWriterConfig(writerConfig);
	                return w;
            	}
            	@Override
            	void evaluate(SailQuery query, BooleanQueryResultHandler handler) {
	                handler.handleBoolean(((BooleanQuery) query).evaluate());
            	}
            };
            return evaluator;
        } else {
        	throw new AssertionError("Unexpected query type: " + sniffedQuery.getClass());
        }
    }

    private void evaluateUpdate(SparqlQuery sparqlQuery, HttpExchange exchange) throws Exception {
        if (sparqlQuery.mapReduce) {
            validateMapReduce(sparqlQuery);
            String updateString = sparqlQuery.getUpdate();
    		HBaseSail sail = (HBaseSail) ((HBaseRepository)repository).getSail();
    		List<HalyardBulkUpdate.JsonInfo> infos = HalyardBulkUpdate.executeUpdate(sail.getConfiguration(), sail.getTableName(), updateString, sparqlQuery.bindings);
    		sendMapReduceResults(exchange, infos);
        } else {
        	List<JsonUpdateInfo> infos = executeUpdate(sparqlQuery);
        	if (infos != null) {
        		sendResults(exchange, infos);
	        } else {
	        	exchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
	        }
        }
       	LOGGER.info("Update successfully processed");
    }

    private List<JsonUpdateInfo> executeUpdate(SparqlQuery sparqlQuery) throws IOException, InvalidRequestException {
        String updateString = sparqlQuery.getUpdate();
        Dataset dataset = sparqlQuery.getDataset();
        ParsedUpdate parsedUpdate;
    	try(SailRepositoryConnection connection = repository.getConnection()) {
    		if (connection.getSailConnection() instanceof ResultTrackingSailConnection) {
    			((ResultTrackingSailConnection)connection.getSailConnection()).setTrackResultSize(sparqlQuery.trackResultSize);
    		}
    		connection.begin();
	        SailUpdate update = (SailUpdate) connection.prepareUpdate(QueryLanguage.SPARQL, updateString, null);
	        addBindings(update, sparqlQuery.getBindings());
	    	parsedUpdate = update.getParsedUpdate();
	        if (!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
	            // This will include default graphs and named graphs from  the request parameters
	        	if (!parsedUpdate.getDatasetMapping().isEmpty()) {
	        		throw new InvalidRequestException("Can't provide graph-uri parameters for queries containing USING, USING NAMED or WITH clauses");
	        	}
	        	for (UpdateExpr expr : parsedUpdate.getUpdateExprs()) {
	        		parsedUpdate.map(expr, dataset);
	        	}
	        }
	        LOGGER.info("Executing update:\nBindings: {}\n{}", sparqlQuery.getBindings(), sparqlQuery.getUpdate());
	        update.execute();
	        connection.commit();
    	}

    	if (sparqlQuery.trackResultSize) {
    		List<JsonUpdateInfo> infos = new ArrayList<>();
	    	for (UpdateExpr expr : parsedUpdate.getUpdateExprs()) {
	    		infos.add(JsonUpdateInfo.from(expr));
	    	}
	    	return infos;
    	} else {
    		return null;
    	}
    }

    private void validateMapReduce(SparqlQuery sparqlQuery) throws InvalidRequestException {
        Dataset dataset = sparqlQuery.getDataset();
        if (!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
        	throw new InvalidRequestException("Map-reduce doesn't support graph-uri parameters");
        }
    }

    private void sendResults(HttpExchange exchange, List<?> infos) throws IOException {
        StringBuilderWriter buf = new StringBuilderWriter(128);
    	JsonGenerator json = new ObjectMapper().createGenerator(buf);
    	json.writeStartObject();
    	json.writeObjectField("results", infos);
    	json.writeEndObject();
    	json.close();
    	buf.close();
    	exchange.getResponseHeaders().set("Content-Type", JSON_CONTENT);
    	sendResponse(exchange, HttpURLConnection.HTTP_OK, buf.toString());
    }

    private void sendMapReduceResults(HttpExchange exchange, List<?> infos) throws IOException {
		if (infos != null) {
	        StringBuilderWriter buf = new StringBuilderWriter(128);
        	JsonGenerator json = new ObjectMapper().createGenerator(buf);
        	json.writeStartObject();
        	json.writeObjectField("jobs", infos);
        	json.writeEndObject();
        	json.close();
        	buf.close();
	    	exchange.getResponseHeaders().set("Content-Type", JSON_CONTENT);
	    	sendResponse(exchange, HttpURLConnection.HTTP_OK, buf.toString());
		} else {
			throw new IOException("Map reduce failed");
		}
    }

    private void loadData(SparqlQuery sparqlQuery, HttpExchange exchange) throws Exception {
       	LOGGER.info("Loading data");
    	try(SailRepositoryConnection connection = repository.getConnection()) {
    		connection.begin();
    		try (InputStream in = exchange.getRequestBody()) {
    			connection.add(in, sparqlQuery.getGraphFormat(), sparqlQuery.getContexts());
    		}
    		connection.commit();
    	}
       	LOGGER.info("Load successful");
    	exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, -1);
    }

    private void clearData(SparqlQuery sparqlQuery, HttpExchange exchange) throws Exception {
       	LOGGER.info("Clearing data");
		try (SailRepositoryConnection conn = repository.getConnection()) {
			conn.clear(sparqlQuery.getContexts());
		}
       	LOGGER.info("Clear successful");
    	exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, -1);
    }

    private void executeQueryTemplate(SparqlQuery sparqlQuery, HttpExchange exchange) throws Exception {
        QueryEvaluator<?,?,?> evaluator = getQueryEvaluator(sparqlQuery.getQuery(), exchange);
    	TupleQueryResultParser parser = QueryResultIO.createTupleParser(sparqlQuery.getFlatFileFormat(), repository.getValueFactory());
    	try (SailRepositoryConnection conn = repository.getConnection()) {
    		conn.begin();
	        SailQuery query = (SailQuery) conn.prepareQuery(QueryLanguage.SPARQL, sparqlQuery.getQuery(), null);
	        addBindings(query, sparqlQuery.getBindings());
	        ParsedQuery parsedQuery = query.getParsedQuery();
	        Dataset dataset = sparqlQuery.getDataset();
	        if (!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
	            // This will include default graphs and named graphs from  the request parameters but default graphs and
	            // named graphs contained in the string query will be ignored
	            parsedQuery.setDataset(dataset);
	        }

	        evaluator.setContentType(exchange);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
            OutputStream response = new BufferedOutputStream(exchange.getResponseBody());
	        QueryTupleQueryResultHandler<?> handler = QueryTupleQueryResultHandler.create(query, evaluator, response);
	    	parser.setQueryResultHandler(handler);
			try (InputStream in = exchange.getRequestBody()) {
		    	parser.parseQueryResult(in);
			}
	    	conn.commit();
    	}
    }

    static final class QueryTupleQueryResultHandler<H> extends AbstractTupleQueryResultHandler {
		private final SailQuery query;
		private List<String> dataBindingNames;
		private final QueryEvaluator<?,?,H> evaluator;
		private final H handler;

		static <H> QueryTupleQueryResultHandler<H> create(SailQuery query, QueryEvaluator<?,?,H> evaluator, OutputStream out) {
			return new QueryTupleQueryResultHandler<>(query, evaluator, out);
		}

		QueryTupleQueryResultHandler(SailQuery query, QueryEvaluator<?,?,H> evaluator, OutputStream out) {
			this.query = query;
			this.evaluator = evaluator;
			H writer = evaluator.getWriter(out);
			if (writer instanceof QueryResultHandler) {
				this.handler = (H) new MergingQueryResultHandler((QueryResultHandler)writer);
			} else if (writer instanceof RDFHandler) {
				this.handler = (H) new MergingRDFHandler((RDFHandler)writer);
			} else {
				throw new AssertionError();
			}
		}

		@Override
		public void startQueryResult(List<String> bindingNames) {
			this.dataBindingNames = bindingNames;
		}

		@Override
		public void handleSolution(BindingSet dataBindings) {
			for (String bn : dataBindingNames) {
				query.removeBinding(bn);
				Value bv = dataBindings.getValue(bn);
				if (bv != null) {
					query.setBinding(bn, bv);
				}
			}
			LOGGER.debug("Evaluating query:\nBindings: {}\n{}", query.getBindings(), query.getParsedQuery().getSourceString());
			evaluator.evaluate(query, handler);
		}

		@Override
		public void endQueryResult() {
			try {
				((Closeable)handler).close();
			} catch (IOException ioe) {
				throw new TupleQueryResultHandlerException(ioe);
			}
		}
    }

    private void executeUpdateTemplate(SparqlQuery sparqlQuery, HttpExchange exchange) throws Exception {
    	TupleQueryResultParser parser = QueryResultIO.createTupleParser(sparqlQuery.getFlatFileFormat(), repository.getValueFactory());
    	List<JsonUpdateInfo> infos = new ArrayList<>();
    	try (SailRepositoryConnection conn = repository.getConnection()) {
    		if (conn.getSailConnection() instanceof ResultTrackingSailConnection) {
    			((ResultTrackingSailConnection)conn.getSailConnection()).setTrackResultSize(sparqlQuery.trackResultSize);
    		}
    		if (conn.getSailConnection() instanceof HBaseSailConnection) {
    			((HBaseSailConnection)conn.getSailConnection()).setFlushWritesBeforeReadsEnabled(false);
    		}
    		conn.begin();
	        SailUpdate update = (SailUpdate) conn.prepareUpdate(QueryLanguage.SPARQL, sparqlQuery.getUpdate(), null);
	        addBindings(update, sparqlQuery.getBindings());
	        ParsedUpdate parsedUpdate = update.getParsedUpdate();
	        Dataset dataset = sparqlQuery.getDataset();
	        if (!dataset.getDefaultGraphs().isEmpty() || !dataset.getNamedGraphs().isEmpty()) {
	            // This will include default graphs and named graphs from  the request parameters
	        	if (!parsedUpdate.getDatasetMapping().isEmpty()) {
	        		throw new InvalidRequestException("Can't provide graph-uri parameters for queries containing USING, USING NAMED or WITH clauses");
	        	}
	        	for (UpdateExpr expr : parsedUpdate.getUpdateExprs()) {
	        		parsedUpdate.map(expr, dataset);
	        	}
	        }

	        UpdateTupleQueryResultHandler handler = new UpdateTupleQueryResultHandler(update);
	    	parser.setQueryResultHandler(handler);
			try (InputStream in = exchange.getRequestBody()) {
		    	parser.parseQueryResult(in);
			}
	    	for (UpdateExpr expr : parsedUpdate.getUpdateExprs()) {
	    		infos.add(JsonUpdateInfo.from(expr));
	    	}
	    	conn.commit();
    	}
    	if (sparqlQuery.trackResultSize) {
	        StringBuilderWriter buf = new StringBuilderWriter(128);
        	JsonGenerator json = new ObjectMapper().createGenerator(buf);
        	json.writeStartObject();
        	json.writeObjectField("results", infos);
	    	json.writeEndObject();
	    	json.close();
	    	buf.close();
	    	exchange.getResponseHeaders().set("Content-Type", JSON_CONTENT);
	    	sendResponse(exchange, HttpURLConnection.HTTP_OK, buf.toString());
        } else {
        	exchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
        }
    }

	static final class UpdateTupleQueryResultHandler extends AbstractTupleQueryResultHandler {
		private final SailUpdate update;
		private List<String> dataBindingNames;

		UpdateTupleQueryResultHandler(SailUpdate update) {
			this.update = update;
		}

		@Override
		public void startQueryResult(List<String> bindingNames) {
			this.dataBindingNames = bindingNames;
		}

		@Override
		public void handleSolution(BindingSet dataBindings) {
			for (String bn : dataBindingNames) {
				update.removeBinding(bn);
				Value bv = dataBindings.getValue(bn);
				if (bv != null) {
					update.setBinding(bn, bv);
				}
			}
			LOGGER.debug("Executing update:\nBindings: {}\n{}", update.getBindings(), update.getParsedUpdate().getSourceString());
			update.execute();
		}
	}

    private void sendManagementData(HttpExchange exchange) throws IOException, InvalidRequestException {
        String path = exchange.getRequestURI().getPath();
        int indent = 0;
        for (NameValuePair queryParam : getQueryParams(exchange)) {
        	if ("pretty".equals(queryParam.getName())) {
        		indent = 2;
        	}
        }

        String domain = path.substring(JMX_ENDPOINT.length());
        if (domain.isEmpty()) {
        	domain = "*";
        }
        ObjectName mbQuery;
        try {
        	mbQuery = ObjectName.getInstance(domain+":*");
        } catch (MalformedObjectNameException e) {
        	throw new InvalidRequestException(e.getMessage());
        }
    	JSONObject json = new JSONObject();
    	MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    	Set<ObjectName> ons = mbs.queryNames(mbQuery, null);
    	for (ObjectName on : ons) {
    		try {
        		MBeanInfo mbi = mbs.getMBeanInfo(on);
        		JSONObject domainBean = json.optJSONObject(on.getDomain());
        		if (domainBean == null) {
        			domainBean = new JSONObject();
        			json.put(on.getDomain(), domainBean);
        		}
        		JSONObject jsonBean = new JSONObject();
        		domainBean.put(on.getKeyPropertyListString(), jsonBean);
        		MBeanAttributeInfo[] mbAttrs = mbi.getAttributes();
        		String[] attrNames = new String[mbAttrs.length];
        		for (int i=0; i<mbAttrs.length; i++) {
        			attrNames[i] = mbAttrs[i].getName();
        		}
        		AttributeList attrValues = mbs.getAttributes(on, attrNames);
        		for (Attribute mbattr : attrValues.asList()) {
        			jsonBean.put(mbattr.getName(), toJson(mbattr.getValue()));
        		}
    		} catch (InstanceNotFoundException | IntrospectionException | ReflectionException  ex) {
    			// ignore/skip
    		}
    	}
    	exchange.getResponseHeaders().set("Content-Type", JSON_CONTENT);
    	sendResponse(exchange, HttpURLConnection.HTTP_OK, json.toString(indent));
    }

    private static Object toJson(Object object) {
    	if (object instanceof CompositeData[]) {
			CompositeData[] arr = (CompositeData[]) object;
    		Object[] out = new Object[arr.length];
    		for (int i=0; i<arr.length; i++) {
    			out[i] = toJson(arr[i]);
    		}
    		return out;
    	} else if (object instanceof CompositeData) {
    		CompositeData data = (CompositeData) object;
    		JSONObject jsonData = new JSONObject();
    		for (String key : data.getCompositeType().keySet()) {
    			jsonData.put(key, toJson(data.get(key)));
    		}
    		return jsonData;
    	} else if (object instanceof TabularData) {
    		TabularData data = (TabularData) object;
    		JSONArray rows = new JSONArray();
    		for (CompositeData row : (Collection<? extends CompositeData>) data.values()) {
    			rows.put(toJson(row));
    		}
    		return rows;
    	} else {
    		return JSONObject.wrap(object);
    	}
    }

    /**
     * Send response of the processed request to the client
     *
     * @param exchange HttpExchange wrapper encapsulating response
     * @param code     HTTP code
     * @param exception  Content of the response
     * @throws IOException
     */
    private void sendErrorResponse(HttpExchange exchange, int code, Exception exception) throws IOException {
        StringWriter sw = new StringWriter();
        try (PrintWriter w = new PrintWriter(sw)) {
            exception.printStackTrace(w);
        }
        sendResponse(exchange, code, sw.toString());
    }
    private void sendResponse(HttpExchange exchange, int code, String s) throws IOException {
        byte[] payload = s.getBytes(CHARSET);
        exchange.sendResponseHeaders(code, payload.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(payload);
        }
    }

    /**
     * Get file format of the response and set its MIME type as a response content-type header value.
     * <p>
     * If no accepted MIME types are specified, a default file format for the default MIME type is chosen.
     *
     * @param reg           requested result writer registry
     * @param path          requested file path
     * @param mimeTypes     requested/accepted response MIME types (e.g. application/sparql-results+xml,
     *                      application/xml)
     * @param defaultFormat default file format (e.g. SPARQL/XML)
     * @param h             response headers for setting response content-type value
     * @param <FF>          file format
     * @param <S>
     * @return
     */
    private static <FF extends FileFormat, S extends Object> FF setFormat(FileFormatServiceRegistry<FF, S> reg,
                                                                   String path, List<String> mimeTypes,
                                                                   FF defaultFormat, Headers h) {
        if (path != null) {
            Optional<FF> o = reg.getFileFormatForFileName(path);
            if (o.isPresent()) {
                Charset chs = o.get().getCharset();
                h.set("Content-Type", o.get().getDefaultMIMEType() + (chs == null ? "" : ("; charset=" + chs.name())));
                return o.get();
            }
        }
        if (mimeTypes != null) {
            for (String mimeType : mimeTypes) {
                Optional<FF> o = reg.getFileFormatForMIMEType(mimeType);
                if (o.isPresent()) {
                    Charset chs = o.get().getCharset();
                    h.set("Content-Type", mimeType + (chs == null ? "" : ("; charset=" + chs.name())));
                    return o.get();
                }
            }
        }
        h.set("Content-Type", defaultFormat.getDefaultMIMEType());
        return defaultFormat;
    }

    /**
     * Parse Accept header in case it contains multiple mime types. Relative quality factors ('q') are removed.
     * <p>
     * Example:
     * Accept header: "text/html, application/xthml+xml, application/xml;q=0.9, image/webp;q=0.1"
     * ---> parse to List<String>:
     * [0] "text/html"
     * [1] "application/xthml+xml"
     * [2] "application/xml"
     * [3] "image/webp"
     *
     * @param acceptHeader
     * @return parsed Accept header
     */
    private static List<String> parseAcceptHeader(String acceptHeader) {
        List<String> mimeTypes = Arrays.asList(acceptHeader.trim().split(","));
        for (int i = 0; i < mimeTypes.size(); i++) {
            mimeTypes.set(i, mimeTypes.get(i).trim().split(";", 0)[0]);
        }
        return mimeTypes;
    }

    private static List<NameValuePair> getQueryParams(HttpExchange exchange) {
        // Retrieve from the request URI optional parameters defaultGraphs and namedGraphs
        // Cannot apply directly exchange.getRequestURI().getQuery() since getQuery() method
        // automatically decodes query (requestQuery must remain unencoded due to parsing by '&' delimiter)
        String requestQueryRaw = exchange.getRequestURI().getRawQuery();
        if (requestQueryRaw != null) {
        	return URLEncodedUtils.parse(requestQueryRaw, CHARSET);
        } else {
        	return Collections.emptyList();
        }
    }

    private static void addBindings(Operation op, Map<String,Value> bindings) {
        for (Map.Entry<String, Value> binding : bindings.entrySet()) {
        	op.setBinding(binding.getKey(), binding.getValue());
        }
    }


    /**
     * Help class for retrieving the whole SPARQL query, including optional parameters defaultGraphs and namedGraphs,
     * from the HTTP request.
     */
    private static final class SparqlQuery {
        // SPARQL query string, has to be exactly one
        private String query, update;
        // SPARQL query template parameters for substitution
        private final Map<String,String> parameters = new HashMap<>();
        private final Map<String,Value> bindings = new HashMap<>();
        private final Set<IRI> defaultGraphs = new HashSet<>();
        private final Set<IRI> namedGraphs = new HashSet<>();
        private boolean defaultGraph;
        private IRI graph;
        private boolean trackResultSize;
        private boolean mapReduce;
        private String target;
        private RDFFormat graphData;
        private QueryResultFormat flatData;

        public String getQuery() {
        	return replaceParameters(query);
        }

        public void setQuery(String query) throws InvalidRequestException {
        	if (this.update != null) {
                throw new InvalidRequestException("Unexpected update string encountered");
        	}
        	if (this.query != null) {
                throw new InvalidRequestException("Multiple query strings encountered");
        	}
            this.query = query;
        }

        public String getUpdate() {
        	return replaceParameters(update);
        }

        public void setUpdate(String update) throws InvalidRequestException {
        	if (this.query != null) {
                throw new InvalidRequestException("Unexpected query string encountered");
        	}
        	if (this.update != null) {
                throw new InvalidRequestException("Multiple update strings encountered");
        	}
            this.update = update;
        }

        private String replaceParameters(String s) {
            //replace all tokens matching {{parameterName}} inside the given SPARQL query with corresponding parameterValues
            String[] tokens = new String[parameters.size()];
            String[] values = new String[parameters.size()];
            int i = 0;
            for (Map.Entry<String,String> param : parameters.entrySet()) {
                tokens[i] = "{{" + param.getKey() + "}}";
                values[i] = param.getValue();
                i++;
            }
            return StringUtils.replaceEach(s, tokens, values);
        }

        public void addDefaultGraph(IRI defaultGraph) {
            defaultGraphs.add(defaultGraph);
        }

        public void addNamedGraph(IRI namedGraph) {
            namedGraphs.add(namedGraph);
        }

        public void addParameter(String name, String value) {
            parameters.put(name, value);
        }

        public void addBinding(String name, Value value) {
        	bindings.put(name, value);
        }

        public Map<String,Value> getBindings() {
        	return Collections.unmodifiableMap(bindings);
        }

        public Dataset getDataset() {
        	return new Dataset() {
				@Override
				public IRI getDefaultInsertGraph() {
					return null;
				}

				@Override
				public Set<IRI> getDefaultRemoveGraphs() {
					return Collections.emptySet();
				}

				@Override
				public Set<IRI> getDefaultGraphs() {
					return Collections.unmodifiableSet(defaultGraphs);
				}

				@Override
				public Set<IRI> getNamedGraphs() {
					return Collections.unmodifiableSet(namedGraphs);
				}
        	};
        }

        public void setDefaultGraph(boolean f) throws InvalidRequestException {
        	if (this.defaultGraph) {
        		throw new InvalidRequestException("Multiple 'default' query parameters encountered");
        	}
    		if (this.graph != null) {
    			throw new InvalidRequestException("Cannot specify both 'default' and 'graph' query parameters");
    		}
        	this.defaultGraph = f;
        }

        public void setGraph(IRI graph) throws InvalidRequestException {
        	if (this.graph != null) {
        		throw new InvalidRequestException("Multiple 'graph' query parameters encountered");
        	}
    		if (this.defaultGraph) {
    			throw new InvalidRequestException("Cannot specify both 'default' and 'graph' query parameters");
    		}
        	this.graph = graph;
        }

        public Resource[] getContexts() {
        	return (graph != null) ? new Resource[] {graph} : new Resource[0];
        }

        public void setGraphFormat(RDFFormat format) {
        	this.graphData = format;
        }

        public RDFFormat getGraphFormat() {
        	return graphData;
        }

        public void setFlatFileFormat(QueryResultFormat format) {
        	this.flatData = format;
        }

        public QueryResultFormat getFlatFileFormat() {
        	return flatData;
        }
    }


    private static abstract class QueryEvaluator<FF extends FileFormat,S,H> {
    	final FileFormatServiceRegistry<FF, S> registry;
    	final FF defaultFormat;
    	S writerFactory;

    	QueryEvaluator(FileFormatServiceRegistry<FF, S> registry, FF defaultFormat) {
    		this.registry = registry;
    		this.defaultFormat = defaultFormat;
    	}

    	void setContentType(HttpExchange exchange) {
            List<String> acceptedMimeTypes = new ArrayList<>();
            List<String> acceptHeaders = exchange.getRequestHeaders().get("Accept");
            if (acceptHeaders != null) for (String header : acceptHeaders) {
                acceptedMimeTypes.addAll(parseAcceptHeader(header));
            }
            FF format = setFormat(registry, exchange.getRequestURI().getPath(),
                    acceptedMimeTypes, defaultFormat, exchange.getResponseHeaders());
            writerFactory = registry.get(format).orElseThrow(() -> new AssertionError("Format not supported: "+format));
    	}

    	final void evaluate(SailQuery query, OutputStream out) throws IOException {
            H writer = getWriter(out);
            evaluate(query, writer);
    	}

    	abstract H getWriter(OutputStream out);
    	abstract void evaluate(SailQuery query, H handler);
    }


    static class JsonExportInfo {
    	public long totalExported;
    }

    static class JsonUpdateInfo {
    	public long totalInserted;
    	public long totalDeleted;

    	static JsonUpdateInfo from(UpdateExpr expr) {
    		if (expr instanceof Modify) {
    			Modify modify = (Modify) expr;
    			JsonUpdateInfo info = new JsonUpdateInfo();
    			if (modify.getDeleteExpr() != null) {
    				long count = modify.getDeleteExpr().getResultSizeActual();
    				if (count > 0L) {
    					info.totalDeleted = count;
    				}
    			}
    			if (modify.getInsertExpr() != null) {
    				long count = modify.getInsertExpr().getResultSizeActual();
    				if (count > 0L) {
    					info.totalInserted = count;
    				}
    			}
				return info;
    		} else if (expr instanceof Clear) {
    			Clear clear = (Clear) expr;
    			JsonUpdateInfo info = new JsonUpdateInfo();
    			// -1 means unknown
				info.totalDeleted = clear.getResultSizeActual();
				return info;
    		} else {
    			return null;
    		}
    	}
    }


    static final class InvalidRequestException extends Exception {
		private static final long serialVersionUID = -4164792385420526471L;

		public InvalidRequestException(String message) {
			super(message);
		}
    }
}
