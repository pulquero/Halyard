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

import com.msd.gin.halyard.repository.HBaseRepository;
import com.msd.gin.halyard.sail.ElasticSettings;
import com.msd.gin.halyard.sail.HBaseSail;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;

/**
 * @author sykorjan
 */
public final class HalyardEndpoint extends AbstractHalyardTool {
    private static final String CONTEXT = "/";
    private static final String HOSTNAME = "http://localhost";

    /**
     * Default port number
     * When port number is zero, system will automatically assign a new free port
     */
    private static final int DEFAULT_PORT = 0;
    /**
     * Default evaluation timeout
     * When timeout is zero, then the evaluation timeout is unlimited
     */
    private static final int DEFAULT_TIMEOUT = 0;

    public HalyardEndpoint() {
        super(
                "endpoint",
                "Halyard Endpoint is a command-line application designed to launch a simple SPARQL Endpoint to serve " +
                        "SPARQL Queries. Any custom commands that are to be run as a new subprocess internally by " +
                        "this tool have to be passed at the end of this tool.\n" +
                        "Endpoint URI is accessible via the system environment variable ENDPOINT.\n" +
                        "Warning: All options following a first unrecognized option are ignored and are considered" +
                        " as part of the custom command.",
                "Example: halyard endpoint -p 8000 -s TABLE --verbose customScript.sh customArg1 customArg2"
        );
        // allow passing unspecified additional parameters for the executable script
        cmdMoreArgs = true;
        addOption(
                "p", "port", "http_server_port", "HTTP server port number. If no port number is specified, system " +
                        "will automatically select a new port number", false, true);
        addOption("s", "source-dataset", "dataset_table", "Source HBase table with Halyard RDF store", false, true);
        addOption("i", "elastic-index", "elastic_index_url", ElasticSettings.ELASTIC_INDEX_URL, "Optional ElasticSearch index URL", false, true);
        addOption("t", "timeout", "evaluation_timeout", "Timeout in seconds for each query evaluation (default is " +
                "unlimited timeout)", false, true);
        addOption("q", "stored-queries", "property_file", "Optional property file with pre-defined stored queries. " +
                "Each property name will be mapped to URL path and each property value represents SPARQL query template " +
                "or @<path> to the file with the query template. " +
                "Query template may contain custom tokens that will be replaced by corresponding request parameter value. " +
                "For example stored queries property file containing: \"my_describe_query=describe <{{my_parameter}}>\" " +
                "will resolve and execute request to /my_describe_query?my_parameter=http%3A%2F%2Fwhatever%2F as " +
                "\"describe <http://whatever/>\" query.", false, true);
        addOption("w", "writer-properties", "property_file", "Optional property file with RDF4J Rio WriterConfig properties. " +
                "Each property name is fully qualified class.field name of WriterSetting and property value is fully qualified " +
                " class.field or enum name with the value to set. For example: " +
                "\"org.eclipse.rdf4j.rio.helpers.JSONLDSettings.JSONLD_MODE=org.eclipse.rdf4j.rio.helpers.JSONLDMode.COMPACT\"", false, true);
        addOption(null, "sail-factory", "class_name", "Specify a factory to use a different SAIL implementation", false, true);
        addKeyValueOption(null, "sail-factory-param", "param=value", null, "Optional parameters to pass to the SAIL factory");
        addOption(null, "create", null, "Create HBase table if it doesn't exist", false, false);
        addOption(null, "pull", null, "Use a pull-based evaluation strategy", false, false);
    }

    /**
     * Parse options and user's custom command, create SPARQL endpoint connected to a specified Sail repository and run
     * user's custom command as a new subprocess.
     *
     * @param cmd list of halyard endpoint tool arguments
     * @return exit value of the created subprocess
     * @throws EndpointException if any error occurs during parsing, setting up server/endpoint or running the
     *                           subprocess
     */
    @Override
    protected int run(CommandLine cmd) throws EndpointException, MissingOptionException {
        try {
            int timeout = parseTimeout(cmd);
            int port = parsePort(cmd);
            String table = cmd.getOptionValue('s');
            configureString(cmd, 'i', null);
            boolean create = cmd.hasOption("create");
            boolean usePush = !cmd.hasOption("pull");
            String sailFactoryClassName = cmd.getOptionValue("sail-factory");

            // Any left-over non-recognized options and arguments are considered as part of user's custom commands
            // that are to be run by this tool
            List<String> cmdArgs = Arrays.asList(cmd.getArgs());

            SailRepository rep;
            if (sailFactoryClassName != null) {
            	if (table != null) {
            		throw new EndpointException("Unexpected -s or --source-data");
            	}

            	EndpointSailFactory factory;
            	try {
            		Class<?> factoryClass = Class.forName(sailFactoryClassName);
            		factory = (EndpointSailFactory) factoryClass.getDeclaredConstructor().newInstance();
            		factory.setConf(getConf());
            		Properties factoryParams = cmd.getOptionProperties("sail-factory-param");
            		for (String key : factoryParams.stringPropertyNames()) {
            			String value = factoryParams.getProperty(key);
            			Method setter = factoryClass.getMethod("set"+Character.toUpperCase(key.charAt(0))+key.substring(1), String.class);
            			setter.invoke(factory, value);
            		}
            	} catch (Exception e) {
            		throw new EndpointException("Failed to instantiate SAIL");
            	}
            	rep = new SailRepository(factory.createSail());
            } else {
            	if (table == null) {
            		throw new MissingOptionException("One of -s or --source-dataset is required");
            	}

            	rep = new HBaseRepository(
	                    new HBaseSail(getConf(), table, create, 0, usePush, timeout, ElasticSettings.from(getConf())));
            }

            rep.init();
            try {
                Properties storedQueries = new Properties();
                if (cmd.hasOption('q')) {
                    String qf = cmd.getOptionValue('q');
                    try (FileInputStream in = new FileInputStream(qf)) {
                        storedQueries.load(in);
                    }
                    storedQueries.replaceAll((Object key, Object value) -> {
                        try {
                            return value.toString().startsWith("@") ? Files.readString(Paths.get(qf).resolveSibling(value.toString().substring(1)), StandardCharsets.UTF_8) : value;
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
                }
                Properties writerConfig = new Properties();
                if (cmd.hasOption('w')) {
                    try (FileInputStream in = new FileInputStream(cmd.getOptionValue('w'))) {
                        writerConfig.load(in);
                    }
                }
                CountDownLatch latch = new CountDownLatch(1);
                SimpleHttpServer server = new SimpleHttpServer(port);
                HttpSparqlHandler handler = new HttpSparqlHandler(rep, storedQueries, writerConfig, latch::countDown);
                server.createContext(CONTEXT, handler);
                server.start();
                try {
                    if (cmdArgs.size() > 0) {
                        ProcessBuilder pb = new ProcessBuilder(cmdArgs).inheritIO();
                        pb.environment().put("ENDPOINT", HOSTNAME + ":" + server.getAddress().getPort() + "/");
                        return pb.start().waitFor();
                    } else {
                        latch.await();
                        return 0;
                    }
                } finally {
                    server.stop();
                }
            } finally {
                rep.shutDown();
            }
        } catch (IOException | InterruptedException e) {
            throw new EndpointException(e);
        }
    }

    /**
     * Parse timeout number
     *
     * @param cmd
     * @return
     * @throws EndpointException Provided timeout value is not a number
     */
    private int parseTimeout(CommandLine cmd) throws EndpointException {
        String timeoutString = cmd.getOptionValue('t');
        if (timeoutString == null) {
            return DEFAULT_TIMEOUT; // When no timeout specified, return default unlimited timeout (zero)
        } else {
            try {
                return Integer.parseInt(timeoutString);
            } catch (NumberFormatException e) {
                throw new EndpointException("Failed to parse timeout number from the input string: " + timeoutString);
            }
        }
    }

    /**
     * Parse port number
     *
     * @param cmd
     * @return
     * @throws EndpointException Provided port value is not a number
     */
    private int parsePort(CommandLine cmd) throws EndpointException {
        String portString = cmd.getOptionValue('p');
        if (portString == null) {
            return DEFAULT_PORT;
        } else {
            try {
                return Integer.parseInt(portString);
            } catch (NumberFormatException e) {
                throw new EndpointException("Failed to parse port number from the input string: " + portString);
            }
        }
    }

    public static interface EndpointSailFactory {
    	void setConf(Configuration conf);
    	Sail createSail();
    }

    /**
     * A generic exception during using Halyard endpoint
     */
    public static final class EndpointException extends Exception {
        private static final long serialVersionUID = -8594289635052879832L;

        public EndpointException(String message) {
            super(message);
        }

        public EndpointException(Throwable cause) {
            super(cause);
        }
    }
}
