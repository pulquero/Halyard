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

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.rio.HRDFParser;
import com.msd.gin.halyard.util.Version;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParserFactory;
import org.eclipse.rdf4j.rio.rdfjson.RDFJSONParserFactory;
import org.eclipse.rdf4j.rio.rdfxml.RDFXMLParserFactory;
import org.eclipse.rdf4j.rio.trig.TriGParserFactory;
import org.eclipse.rdf4j.rio.trix.TriXParserFactory;
import org.eclipse.rdf4j.rio.turtle.TurtleParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.langchain4j.model.embedding.EmbeddingModel;

/**
 *
 * @author Adam Sotona (MSD)
 */
public abstract class AbstractHalyardTool implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHalyardTool.class);

    protected static String confProperty(String tool, String key) {
        return "halyard-tools."+tool+"."+key;
    }

    private static final String SOURCE_PROPERTIES = "source";
    protected static final String SOURCE_PATHS_PROPERTY = confProperty(SOURCE_PROPERTIES, "paths");
    protected static final String SOURCE_NAME_PROPERTY = confProperty(SOURCE_PROPERTIES, "name");
    protected static final String SNAPSHOT_PATH_PROPERTY = confProperty(SOURCE_PROPERTIES, "snapshot");
    protected static final String DRY_RUN_PROPERTY = "halyard-tools.dry-run";
    protected static final String BINDING_PROPERTY_PREFIX = "halyard-tools.binding.";

    private Configuration conf;
    final String name, header, footer;
    private final Options options = new Options();
    private final List<String> singleOptions = new ArrayList<>();
    private int opts = 0;
    /**
     * Allow to pass additional unspecified arguments via command line. By default this functionality is disabled.
     * This functionality is used by HalyardEndpoint tool to pass additional arguments for an inner process.
     */
    protected boolean cmdMoreArgs = false;

    protected AbstractHalyardTool(String name, String header, String footer) {
        this.name = name;
        this.header = header;
        this.footer = footer;
        addOption("h", "help", null, "Prints this help", false, false);
        addOption("v", "version", null, "Prints version", false, false);
    }

    protected final void printHelp() {
        HelpFormatter hf = new HelpFormatter();
        hf.setOptionComparator(new Comparator<Option>() {
            @Override
            public int compare(Option o1, Option o2) {
                if (o1 instanceof OrderedOption && o2 instanceof OrderedOption) {
                	return ((OrderedOption)o1).order - ((OrderedOption)o2).order;
                } else {
                	return 0;
                }
            }
        });
        hf.printHelp(100, "halyard " + name, header, options, footer, true);
    }

    @Override
    public final Configuration getConf() {
        return this.conf;
    }

    @Override
    public final void setConf(final Configuration c) {
        this.conf = c;
    }

    protected static String[] validateIRIs(String... iris) {
    	for (String iri : iris) {
    		try {
				new URI(iri).isAbsolute();
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException("Invalid IRI: "+iri, e);
			}
    	}
    	return iris;
    }

    protected void configureIRI(CommandLine cmd, char opt, String defaultValue) {
    	configureStrings(cmd, opt, defaultValue, v -> {
    		validateIRIs(v);
    	});
    }

    protected void configureIRIPattern(CommandLine cmd, char opt, String defaultValue) {
    	configureString(cmd, opt, defaultValue);
    }

    protected void configureString(CommandLine cmd, char opt, String defaultValue) {
    	configureStrings(cmd, opt, defaultValue, null);
    }

    protected void configureString(CommandLine cmd, char opt, String defaultValue, Consumer<String> valueChecker) {
    	configureStrings(cmd, opt, defaultValue, valueChecker);
    }

    protected void configureStrings(CommandLine cmd, char opt, String defaultValue) {
    	configureStrings(cmd, opt, defaultValue, null);
    }

    protected void configureStrings(CommandLine cmd, char opt, String defaultValue, Consumer<String> valueChecker) {
    	OrderedOption option = (OrderedOption) options.getOption(Character.toString(opt));
    	// command line args always override
    	if (cmd.hasOption(opt)) {
    		String[] values = cmd.getOptionValues(opt);
    		if (valueChecker != null) {
    			for (String value : values) {
    				valueChecker.accept(value);
    			}
    		}
    		setStrings(conf, option.confProperty, Arrays.asList(values));
    	} else if (defaultValue != null) {
    		conf.setIfUnset(option.confProperty, String.valueOf(defaultValue));
    	}
    }

    protected static void setStrings(Configuration conf, String name, Iterable<String> values) {
		conf.set(name, String.join(" ", values));
    }

    protected static String[] getStrings(Configuration conf, String name) {
		String v = conf.get(name);
		return StringUtils.isNotBlank(v) ? v.trim().split("\\s+") : new String[0];
    }

    protected void configureBoolean(CommandLine cmd, char opt) {
    	configureBoolean(cmd, Character.toString(opt));
    }

    protected void configureBoolean(CommandLine cmd, String longOpt) {
    	OrderedOption option = (OrderedOption) options.getOption(longOpt);
    	// command line args always override
    	if (cmd.hasOption(longOpt)) {
    		conf.setBoolean(option.confProperty, true);
    	}
    }

    protected void configureInt(CommandLine cmd, char opt, int defaultValue) {
    	configureInt(cmd, opt, defaultValue, null);
    }

    protected void configureInt(CommandLine cmd, char opt, int defaultValue, IntConsumer valueChecker) {
    	OrderedOption option = (OrderedOption) options.getOption(Character.toString(opt));
    	// command line args always override
    	if (cmd.hasOption(opt)) {
    		int value = Integer.parseInt(cmd.getOptionValue(opt));
    		if (valueChecker != null) {
    			valueChecker.accept(value);
    		}
    		conf.setInt(option.confProperty, value);
    	} else {
    		conf.setIfUnset(option.confProperty, String.valueOf(defaultValue));
    	}
    }

    protected void configureLong(CommandLine cmd, char opt, long defaultValue) {
    	OrderedOption option = (OrderedOption) options.getOption(Character.toString(opt));
    	// command line args always override
    	if (cmd.hasOption(opt)) {
    		conf.setLong(option.confProperty, Long.parseLong(cmd.getOptionValue(opt)));
    	} else {
    		conf.setIfUnset(option.confProperty, String.valueOf(defaultValue));
    	}
    }

    protected void configureBindings(CommandLine cmd, char opt) {
	    Properties bindings = cmd.getOptionProperties(Character.toString(opt));
	    for (String name : bindings.stringPropertyNames()) {
	    	String value = bindings.getProperty(name);
	    	// validate value
	    	NTriplesUtil.parseValue(value, SimpleValueFactory.getInstance());
	    	getConf().set(BINDING_PROPERTY_PREFIX+name, value);
	    }
    }

    protected static BindingSet getBindings(Configuration conf, ValueFactory vf) {
        Map<String,String> bindingProps = conf.getPropsWithPrefix(BINDING_PROPERTY_PREFIX);
    	QueryBindingSet bindingSet = new QueryBindingSet(bindingProps.size()+1);
        for (Map.Entry<String,String> binding : bindingProps.entrySet()) {
        	bindingSet.setBinding(binding.getKey(), NTriplesUtil.parseValue(binding.getValue(), vf));
        }
        return bindingSet;
    }

    protected final void addOption(String opt, String longOpt, String argName, String description, boolean required, boolean single) {
    	addOption(opt, longOpt, argName, null, description, required, single);
    }

    protected final void addOption(String opt, String longOpt, String argName, String confProperty, String description, boolean required, boolean single) {
        Option o = new OrderedOption(opts++, opt, longOpt, argName, confProperty, description, required);
        options.addOption(o);
        if (single) {
            singleOptions.add(opt == null ? longOpt : opt);
        }
    }

    protected final void addKeyValueOption(String opt, String longOpt, String argName, String confProperty, String description) {
        Option o = new OrderedOption(opts++, opt, longOpt, argName, confProperty, description);
        options.addOption(o);
    }

    protected final Collection<Option> getOptions() {
        return options.getOptions();
    }

    protected final List<Option> getRequiredOptions() {
        List<?> optionNames = options.getRequiredOptions();
        List<Option> requiredOptions = new ArrayList<>(optionNames.size());
        for(Object name : optionNames) {
            requiredOptions.add(options.getOption((String) name));
        }
        return requiredOptions;
    }

    protected static boolean isDryRun(Configuration conf) {
    	return conf.getBoolean(DRY_RUN_PROPERTY, false);
    }

    protected static void addRioDependencies(Configuration conf) throws IOException {
    	TableMapReduceUtil.addDependencyJarsForClasses(conf,
			TurtleParserFactory.class,
			TriXParserFactory.class,
			TriGParserFactory.class,
			NTriplesParserFactory.class,
			NQuadsParserFactory.class,
			RDFXMLParserFactory.class,
			RDFJSONParserFactory.class,
			HRDFParser.Factory.class
    	);
    }

    protected static void addLangModelDependencies(Configuration conf) throws IOException {
    	TableMapReduceUtil.addDependencyJarsForClasses(conf,
			EmbeddingModel.class,
			dev.langchain4j.model.embedding.onnx.AbstractInProcessEmbeddingModel.class,
			dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel.class,
			ai.onnxruntime.OrtEnvironment.class,
			dev.langchain4j.model.localai.LocalAiEmbeddingModel.class,
			dev.ai4j.openai4j.OpenAiClient.class,
			dev.langchain4j.model.ollama.OllamaEmbeddingModel.class
    	);
    }

    protected static void bulkLoad(Job job, TableName tableName, Path workDir) throws IOException {
    	// ensure job configuration is used
    	Configuration conf = job.getConfiguration();
    	if (isDryRun(conf)) {
    		LOG.info("Skipping bulk load - dry run");
    	} else {
			// reqd if HFiles need splitting (code from HFileOutputFormat2)
    		addBloomFilterConfig(conf, tableName);
			BulkLoadHFiles.create(conf).bulkLoad(tableName, workDir);
    	}
    }

    protected static void addBloomFilterConfig(Configuration conf, TableName tableName) {
		byte[] tableAndFamily = HalyardTableUtils.getTableNameSuffixedWithFamily(tableName.toBytes());
		Map<byte[], String> bloomTypeMap = createFamilyConfValueMap(conf, "hbase.hfileoutputformat.families.bloomtype");
		String bloomType = bloomTypeMap.get(tableAndFamily);
		if (bloomType == null) {
			throw new IllegalStateException("Missing bloom filter configuration");
		}
		if (BloomType.ROWPREFIX_FIXED_LENGTH.toString().equals(bloomType)) {
			Map<byte[], String> bloomParamMap = createFamilyConfValueMap(conf, "hbase.hfileoutputformat.families.bloomparam");
			String bloomParam = bloomParamMap.get(tableAndFamily);
			conf.set(BloomFilterUtil.PREFIX_LENGTH_KEY, bloomParam);
		}
    }

    private static Map<byte[], String> createFamilyConfValueMap(Configuration conf, String confName) {
        Map<byte[], String> confValMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        String confVal = conf.get(confName, "");
        for (String familyConf : confVal.split("&")) {
          String[] familySplit = familyConf.split("=");
          if (familySplit.length != 2) {
            continue;
          }
          try {
            confValMap.put(Bytes.toBytes(URLDecoder.decode(familySplit[0], "UTF-8")),
              URLDecoder.decode(familySplit[1], "UTF-8"));
          } catch (UnsupportedEncodingException e) {
            // will not happen with UTF-8 encoding
            throw new AssertionError(e);
          }
        }
        return confValMap;
      }


    private static final class OrderedOption extends Option {
    	static String buildDescription(String desc, String confProperty) {
    		 return (confProperty != null) ? desc+" (configuration file property: "+confProperty+")" : desc;
    	}

    	final int order;
    	final String confProperty;
        public OrderedOption(int order, String opt, String longOpt, String argName, String confProperty, String description, boolean required) {
            super(opt, longOpt, argName != null, buildDescription(description, confProperty));
            setArgName(argName);
            setRequired(required);
            this.order = order;
            this.confProperty = confProperty;
        }

        /**
         * Key-value option.
         */
        public OrderedOption(int order, String opt, String longOpt, String argName, String confProperty, String description) {
            super(opt, longOpt, false, buildDescription(description, confProperty));
            setArgName(argName);
            setArgs(Option.UNLIMITED_VALUES);
            setValueSeparator('=');
            this.order = order;
            this.confProperty = confProperty;
        }
    }

    protected abstract int run(CommandLine cmd) throws Exception;

    @Override
    public final int run(String[] args) throws Exception {
        try {
            CommandLine cmd = new PosixParser(){
                @Override
                protected void checkRequiredOptions() throws MissingOptionException {
                    if (!cmd.hasOption('h') && !cmd.hasOption('v')) {
                        super.checkRequiredOptions();
                    }
                }
            }.parse(options, args, cmdMoreArgs);
            if (args.length == 0 || cmd.hasOption('h')) {
                printHelp();
                return -1;
            }
            if (cmd.hasOption('v')) {
                System.out.println("halyard " + name + " " + Version.getVersionString());
                return 0;
            }
            if (!cmdMoreArgs && !cmd.getArgList().isEmpty()) {
                throw new ParseException("Unknown arguments: " + cmd.getArgList().toString());
            }
            for (String opt : singleOptions) {
                String s[] = cmd.getOptionValues(opt);
                if (s != null && s.length > 1)  throw new ParseException("Multiple values for option: " + opt);
            }
            LOG.info("halyard {} {}", name, Version.getVersionString());
            return run(cmd);
        } catch (Exception exp) {
            System.out.println(exp.getMessage());
            printHelp();
            throw exp;
        }
    }

    final static RDFFactory loadRDFFactory(Keyspace keyspace) throws IOException {
    	try (KeyspaceConnection kc = keyspace.getConnection()) {
    		return RDFFactory.create(kc);
    	}
    }

    static class RdfTableMapper<K,V> extends TableMapper<K,V> {
        protected Keyspace keyspace;
        protected KeyspaceConnection keyspaceConn;
        protected RDFFactory rdfFactory;
        protected ValueFactory vf;
        protected StatementIndices stmtIndices;

        protected final void openKeyspace(Configuration conf, String source, String restorePath) throws IOException {
            keyspace = HalyardTableUtils.getKeyspace(conf, source, restorePath);
            keyspaceConn = keyspace.getConnection();
            rdfFactory = RDFFactory.create(keyspaceConn);
            vf = new IdValueFactory(rdfFactory);
            stmtIndices = new StatementIndices(conf, rdfFactory);
        }

        protected void closeKeyspace() throws IOException {
            if (keyspaceConn != null) {
            	keyspaceConn.close();
            	keyspaceConn = null;
            }
            if (keyspace != null) {
                keyspace.close();
                keyspace = null;
            }
        }
    }


    static class RdfReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
        protected Keyspace keyspace;
        protected KeyspaceConnection keyspaceConn;
        protected RDFFactory rdfFactory;
        protected ValueFactory vf;
        protected StatementIndices stmtIndices;

        protected final void openKeyspace(Configuration conf, String source, String restorePath) throws IOException {
            keyspace = HalyardTableUtils.getKeyspace(conf, source, restorePath);
            keyspaceConn = keyspace.getConnection();
            rdfFactory = RDFFactory.create(keyspaceConn);
            vf = new IdValueFactory(rdfFactory);
            stmtIndices = new StatementIndices(conf, rdfFactory);
        }

        protected void closeKeyspace() throws IOException {
            if (keyspaceConn != null) {
            	keyspaceConn.close();
            	keyspaceConn = null;
            }
            if (keyspace != null) {
                keyspace.close();
                keyspace = null;
            }
        }
    }
}
