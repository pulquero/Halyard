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
package com.msd.gin.halyard.sail;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.federation.SailFederatedService;
import com.msd.gin.halyard.function.DynamicFunctionRegistry;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.StatementPatternCardinalityCalculator;
import com.msd.gin.halyard.spin.SpinFunctionInterpreter;
import com.msd.gin.halyard.spin.SpinMagicPropertyInterpreter;
import com.msd.gin.halyard.spin.SpinParser;
import com.msd.gin.halyard.spin.SpinParser.Input;
import com.msd.gin.halyard.util.MBeanDetails;
import com.msd.gin.halyard.util.MBeanManager;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContextInitializer;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * HBaseSail is the RDF Storage And Inference Layer (SAIL) implementation on top of Apache HBase.
 * It implements the interfaces - {@code Sail, SailConnection} and {@code FederatedServiceResolver}. Currently federated queries are
 * only supported for queries across multiple graphs in one Halyard database.
 * @author Adam Sotona (MSD)
 */
public class HBaseSail implements BindingSetCallbackSail, HBaseSailMXBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSail.class);

    /**
     * Ticker is a simple service interface that is notified when some data are processed.
     * It's purpose is to notify a caller (for example MapReduce task) that the execution is still alive.
     */
    public interface Ticker {

        /**
         * This method is called whenever a new Statement is populated from HBase.
         */
        public void tick();
    }

	/**
	 * Interface to make it easy to change connection implementations.
	 */
	public interface SailConnectionFactory {
		HBaseSailConnection createConnection(HBaseSail sail) throws IOException;
	}

	public static final class ScanSettings {
		long minTimestamp = 0;
		long maxTimestamp = Long.MAX_VALUE;
		int maxVersions = 1;

		public long getMinTimestamp() {
			return minTimestamp;
		}

		public long getMaxTimestamp() {
			return maxTimestamp;
		}

		public int getMaxVersions() {
			return maxVersions;
		}
	}

	public static final class QueryInfo {
		private final long startTimestamp = System.currentTimeMillis();
		private Long endTimestamp;
		private final String queryString;
		private final TupleExpr queryExpr;
		private final TupleExpr optimizedExpr;

		public QueryInfo(String queryString, TupleExpr queryExpr, TupleExpr optimizedExpr) {
			this.queryString = queryString;
			this.queryExpr = queryExpr;
			this.optimizedExpr = optimizedExpr;
		}

		public long getStartTimestamp() {
			return startTimestamp;
		}

		public Long getEndTimestamp() {
			return endTimestamp;
		}

		public String getQueryString() {
			return queryString;
		}

		public String getQueryTree() {
			return queryExpr.toString();
		}

		public String getOptimizedQueryTree() {
			return optimizedExpr.toString();
		}

		public boolean isRunning() {
			return endTimestamp == null;
		}

		void end() {
			endTimestamp = System.currentTimeMillis();
		}

		@Override
		public String toString() {
			return "Query: " + queryString + "\nTree:\n" + queryExpr + "\nOptimized:\n" + optimizedExpr;
		}
	}

	private static final long STATUS_CACHING_TIMEOUT = 60000l;

    private final Configuration config; //the configuration of the HBase database
	final TableName tableName;
	final String snapshotName;
	final Path snapshotRestorePath;
	final boolean create;
	final boolean pushStrategy;
	final int splitBits;
	final int evaluationTimeoutSecs;
	private boolean readOnly = true;
	private long readOnlyTimestamp = 0L;
	final ElasticSettings esSettings;
	ElasticsearchTransport esTransport;
	boolean includeNamespaces = false;
	int queryCacheSize;
	private boolean trackResultSize;
	private boolean trackResultTime;
    final Ticker ticker;
	private FederatedServiceResolver federatedServiceResolver;
	private RDFFactory rdfFactory;
	private StatementIndices stmtIndices;
	private ValueFactory valueFactory;
	private final FunctionRegistry functionRegistry = new DynamicFunctionRegistry();
	private final TupleFunctionRegistry tupleFunctionRegistry = TupleFunctionRegistry.getInstance();
	private final SpinParser spinParser = new SpinParser(Input.TEXT_FIRST, functionRegistry, tupleFunctionRegistry);
	private final List<QueryContextInitializer> queryContextInitializers = new ArrayList<>();
	private final ScanSettings scanSettings = new ScanSettings();
	final SailConnectionFactory connFactory;
	Connection hConnection;
	final boolean hConnectionIsShared; //whether a Connection is provided or we need to create our own
	Keyspace keyspace;
	String owner;
	private MBeanManager<HBaseSail> mbeanManager;
	private final Cache<HBaseSailConnection, Object> connInfos = CacheBuilder.newBuilder().weakKeys().removalListener((RemovalNotification<HBaseSailConnection, Object> notif) -> {
		HBaseSailConnection conn = notif.getKey();
		if (notif.wasEvicted()) {
			LOGGER.warn("Closing unreferenced connection {}", conn);
			conn.close();
		} else if (conn.isOpen()) {
			LOGGER.warn("Closing active connection {}", conn);
			conn.close();
		}
	}).build();
	private final Queue<QueryInfo> queryHistory = new ArrayBlockingQueue<>(10, true);

	/**
	 * Property defining optional ElasticSearch index URL
	 */
	public static final String ELASTIC_INDEX_URL = "halyard.elastic.index.url";


	public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings) {
		this(null, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, null, HBaseSailConnection.Factory.INSTANCE);
	}

	/**
	 * Construct HBaseSail for a table.
	 * 
	 * @param conn
	 * @param config
	 * @param tableName
	 * @param create
	 * @param splitBits
	 * @param pushStrategy
	 * @param evaluationTimeout
	 * @param elasticSettings
	 * @param ticker
	 * @param connFactory
	 * @param fsr
	 */
	HBaseSail(@Nullable Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker, SailConnectionFactory connFactory,
			FederatedServiceResolver fsr) {
		this.hConnection = conn;
		this.hConnectionIsShared = (conn != null);
		this.config = Objects.requireNonNull(config);
		this.tableName = TableName.valueOf(tableName);
		this.create = create;
		this.splitBits = splitBits;
		this.snapshotName = null;
		this.snapshotRestorePath = null;
		this.pushStrategy = pushStrategy;
		this.evaluationTimeoutSecs = evaluationTimeout;
		this.esSettings = elasticSettings;
		this.ticker = ticker;
		this.connFactory = connFactory;
		this.federatedServiceResolver = fsr;
		initSettings(config);
	}

	HBaseSail(Configuration config, String snapshotName, String snapshotRestorePath, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings) {
		this(config, snapshotName, snapshotRestorePath, pushStrategy, evaluationTimeout, elasticSettings, null, HBaseSailConnection.Factory.INSTANCE,
				new HBaseFederatedServiceResolver(null, config, null, pushStrategy, evaluationTimeout, null));
	}

	/**
	 * Construct HBaseSail for a snapshot.
	 * 
	 * @param conn
	 * @param config
	 * @param tableName
	 * @param create
	 * @param splitBits
	 * @param pushStrategy
	 * @param evaluationTimeout
	 * @param elasticSettings
	 * @param ticker
	 * @param connFactory
	 * @param fsr
	 */
	HBaseSail(Configuration config, String snapshotName, String snapshotRestorePath, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker, SailConnectionFactory connFactory,
			FederatedServiceResolver fsr) {
		this.hConnection = null;
		this.hConnectionIsShared = false;
		this.config = Objects.requireNonNull(config);
		this.tableName = null;
		this.create = false;
		this.splitBits = -1;
		this.snapshotName = snapshotName;
		this.snapshotRestorePath = new Path(snapshotRestorePath);
		this.pushStrategy = pushStrategy;
		this.evaluationTimeoutSecs = evaluationTimeout;
		this.esSettings = elasticSettings;
		this.ticker = ticker;
		this.connFactory = connFactory;
		this.federatedServiceResolver = fsr;
		initSettings(config);
	}

	private HBaseSail(@Nullable Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker,
			SailConnectionFactory connFactory) {
		this(conn, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, ticker, connFactory,
				new HBaseFederatedServiceResolver(conn, config, tableName, pushStrategy, evaluationTimeout, null));
	}

	HBaseSail(@Nonnull Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker) {
		this(conn, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

	public HBaseSail(@Nonnull Connection conn, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker) {
		this(conn, conn.getConfiguration(), tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

	public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker) {
		this(null, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

    /**
	 * Construct HBaseSail object with given arguments.
	 * 
	 * @param config Hadoop Configuration to access HBase
	 * @param tableName HBase table name used to store data
	 * @param create boolean option to create the table if it does not exist
	 * @param splitBits int number of bits used for the calculation of HTable region pre-splits (applies for new tables only)
	 * @param pushStrategy boolean option to use {@link com.msd.gin.halyard.strategy.HalyardEvaluationStrategy} instead of
	 * {@link org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy}
	 * @param evaluationTimeout int timeout in seconds for each query evaluation, negative values mean no timeout
	 * @param elasticSettings optional ElasticSearch settings
	 * @param ticker optional Ticker callback for keep-alive notifications
	 * @param connFactory {@link SailConnectionFactory} for creating connections
	 */
	public HBaseSail(Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker, SailConnectionFactory connFactory) {
		this(null, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, ticker, connFactory);
    }

	private void initSettings(Configuration config) {
		queryCacheSize = config.getInt(EvaluationConfig.QUERY_CACHE_MAX_SIZE, 100);
		trackResultSize = config.getBoolean(EvaluationConfig.TRACK_RESULT_SIZE, false);
		trackResultTime = config.getBoolean(EvaluationConfig.TRACK_RESULT_TIME, false);
	}

	@Override
	public boolean isPushStrategyEnabled() {
		return pushStrategy;
	}

	@Override
	public int getEvaluationTimeout() {
		return evaluationTimeoutSecs;
	}

	@Override
	public ElasticSettings getElasticSettings() {
		return esSettings;
	}

	@Override
	public int getValueIdentifierSize() {
		return rdfFactory.getIdSize();
	}

	@Override
	public String getValueIdentifierAlgorithm() {
		return rdfFactory.getIdAlgorithm();
	}

	@Override
	public ScanSettings getScanSettings() {
		return scanSettings;
	}

	@Override
	public int getConnectionCount() {
		return (int) connInfos.size();
	}

	@Override
	public boolean isTrackResultSize() {
		return trackResultSize;
	}

	@Override
	public void setTrackResultSize(boolean f) {
		trackResultSize = f;
	}

	@Override
	public boolean isTrackResultTime() {
		return trackResultTime;
	}

	@Override
	public void setTrackResultTime(boolean f) {
		trackResultTime = f;
	}

	@Override
	public QueryInfo[] getRecentQueries() {
		return queryHistory.toArray(new QueryInfo[queryHistory.size()]);
	}

	BufferedMutator getBufferedMutator() {
		if (hConnection == null) {
			throw new SailException("Snapshots are not modifiable");
		}
		try {
			return hConnection.getBufferedMutator(tableName);
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	QueryInfo trackQuery(String sourceString, TupleExpr rawExpr, TupleExpr optimizedExpr) {
		QueryInfo query = new QueryInfo(sourceString, rawExpr, optimizedExpr);
		synchronized (queryHistory) {
			if (!queryHistory.offer(query)) {
				// full - so remove and add
				queryHistory.remove();
				queryHistory.add(query);
			}
		}
		return query;
	}

	/**
	 * Not used in Halyard
	 */
	@Override
	public void setDataDir(File dataDir) {
	}

	/**
	 * Not used in Halyard
	 */
	@Override
	public File getDataDir() {
		throw new UnsupportedOperationException();
	}

	HalyardEvaluationStatistics newStatistics() {
		return newStatistics(HalyardStatsBasedStatementPatternCardinalityCalculator.newStatementCountCache());
	}

	HalyardEvaluationStatistics newStatistics(Cache<IRI, Long> tripleCountCache) {
		StatementPatternCardinalityCalculator.Factory spcalcFactory = () -> new HalyardStatsBasedStatementPatternCardinalityCalculator(new HBaseTripleSource(keyspace.getConnection(), valueFactory, stmtIndices, evaluationTimeoutSecs),
				rdfFactory, tripleCountCache);
		HalyardEvaluationStatistics.ServiceStatsProvider srvStatsProvider = new HalyardEvaluationStatistics.ServiceStatsProvider() {
			final Map<String, Optional<HalyardEvaluationStatistics>> serviceToStats = new HashMap<>();

			public HalyardEvaluationStatistics getStatsForService(String serviceUrl) {
				return serviceToStats.computeIfAbsent(serviceUrl, (service) -> {
					FederatedService fedServ = federatedServiceResolver.getService(service);
					if (fedServ instanceof SailFederatedService) {
						Sail sail = ((SailFederatedService) fedServ).getSail();
						if (sail instanceof HBaseSail) {
							return Optional.of(((HBaseSail) sail).newStatistics());
						}
					}
					return Optional.empty();
				}).orElse(null);
			}
		};
		return new HalyardEvaluationStatistics(spcalcFactory, srvStatsProvider);
	}

	@Override
	public void init() throws SailException {
		try {
			if (tableName != null) {
				if (!hConnectionIsShared) {
					// connections are thread-safe and very heavyweight - only do it once
					if (hConnection != null) {
						throw new IllegalStateException("Sail has already been initialized");
					}
					hConnection = HalyardTableUtils.getConnection(config);
				}
				if (create) {
					HalyardTableUtils.createTableIfNotExists(hConnection, tableName, splitBits);
				}
			}

			keyspace = HalyardTableUtils.getKeyspace(config, hConnection, tableName, snapshotName, snapshotRestorePath);
			try (KeyspaceConnection keyspaceConn = keyspace.getConnection()) {
				rdfFactory = RDFFactory.create(keyspaceConn);
			}
		} catch (IOException e) {
			throw new SailException(e);
		}
		stmtIndices = new StatementIndices(config, rdfFactory);
		valueFactory = new IdValueFactory(rdfFactory);

		if (includeNamespaces) {
			try (HBaseSailConnection conn = getConnection()) {
				conn.addNamespaces();
			}
		}

		SpinFunctionInterpreter.registerSpinParsingFunctions(spinParser, functionRegistry, pushStrategy ? tupleFunctionRegistry : TupleFunctionRegistry.getInstance());
		SpinMagicPropertyInterpreter.registerSpinParsingTupleFunctions(spinParser, tupleFunctionRegistry);

		if (esSettings != null) {
			RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(esSettings.host, esSettings.port != -1 ? esSettings.port : 9200, esSettings.protocol));
			CredentialsProvider esCredentialsProvider;
			if (esSettings.password != null) {
				esCredentialsProvider = new BasicCredentialsProvider();
				esCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esSettings.username, esSettings.password));
			} else {
				esCredentialsProvider = null;
			}
			SSLContext sslContext;
			if (esSettings.sslSettings != null) {
				try {
					sslContext = esSettings.sslSettings.createSSLContext();
				} catch (IOException | GeneralSecurityException e) {
					throw new SailException(e);
				}
			} else {
				sslContext = null;
			}
			restClientBuilder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
				@Override
				public HttpAsyncClientBuilder customizeHttpClient(
						HttpAsyncClientBuilder httpClientBuilder) {
					if (esCredentialsProvider != null) {
						httpClientBuilder.setDefaultCredentialsProvider(esCredentialsProvider);
					}
					if (sslContext != null) {
						httpClientBuilder.setSSLContext(sslContext);
					}
					return httpClientBuilder;
				}
			});
			RestClient restClient = restClientBuilder.build();
			esTransport = new RestClientTransport(restClient, new JacksonJsonpMapper());
		}

		mbeanManager = new MBeanManager<>() {
			@Override
			protected List<MBeanDetails> mbeans(HBaseSail sail) {
				Hashtable<String, String> attrs = new Hashtable<>();
				attrs.putAll(getConnectionAttributes(owner));
				attrs.put("federatedServiceResolver", MBeanManager.getId(federatedServiceResolver));
				return Collections.singletonList(new MBeanDetails(sail, HBaseSailMXBean.class, attrs));
			}
		};
		mbeanManager.register(this);
	}

	Map<String, String> getConnectionAttributes(String owner) {
		Map<String, String> attrs = new HashMap<>();
		if (tableName != null) {
			attrs.put("table", tableName.getNameAsString());
		} else {
			attrs.put("snapshot", snapshotName);
		}
		if (owner != null) {
			attrs.put("owner", owner);
		}
		return attrs;
	}

	private boolean isInitialized() {
		return (keyspace != null) && (rdfFactory != null) && (stmtIndices != null);
	}

	public Configuration getConfiguration() {
		return config;
	}

	public FunctionRegistry getFunctionRegistry() {
		return functionRegistry;
	}

	public TupleFunctionRegistry getTupleFunctionRegistry() {
		return tupleFunctionRegistry;
	}

	public FederatedServiceResolver getFederatedServiceResolver() {
		return federatedServiceResolver;
	}

	public void addQueryContextInitializer(QueryContextInitializer initializer) {
		this.queryContextInitializers.add(initializer);
	}

	protected List<QueryContextInitializer> getQueryContextInitializers() {
		return this.queryContextInitializers;
	}

	public SpinParser getSpinParser() {
		return spinParser;
	}

	public RDFFactory getRDFFactory() {
		if (rdfFactory == null) {
			throw new IllegalStateException("Sail is not initialized");
		}
		return rdfFactory;
	}

	public StatementIndices getStatementIndices() {
		if (stmtIndices == null) {
			throw new IllegalStateException("Sail is not initialized");
		}
		return stmtIndices;
	}

    @Override
	public void shutDown() throws SailException {
		connInfos.invalidateAll();

		if (mbeanManager != null) {
			mbeanManager.unregister();
			mbeanManager = null;
		}

		if (esTransport != null) {
			try {
				esTransport.close();
			} catch (IOException ignore) {
			}
			esTransport = null;
		}
		if (!hConnectionIsShared) {
			if (federatedServiceResolver instanceof AbstractFederatedServiceResolver) {
				((AbstractFederatedServiceResolver) federatedServiceResolver).shutDown();
			}

			if (hConnection != null) {
				try {
					hConnection.close();
				} catch (IOException ignore) {
				}
				hConnection = null;
			}
		}
		if (keyspace != null) {
			try {
				keyspace.destroy();
			} catch (IOException ignore) {
			}
			keyspace = null;
		}
    }

    @Override
    public boolean isWritable() throws SailException {
		if (hConnection != null) {
			long time = System.currentTimeMillis();
			if ((readOnlyTimestamp == 0) || (time > readOnlyTimestamp + STATUS_CACHING_TIMEOUT)) {
				try (Table table = hConnection.getTable(tableName)) {
					readOnly = table.getDescriptor().isReadOnly();
					readOnlyTimestamp = time;
				} catch (IOException ex) {
					throw new SailException(ex);
				}
			}
		}
        return !readOnly;
    }

    @Override
	public HBaseSailConnection getConnection() throws SailException {
		return getConnection(connFactory);
    }

	HBaseSailConnection getConnection(SailConnectionFactory connectionFactory) throws SailException {
		if (!isInitialized()) {
			throw new IllegalStateException("Sail is not initialized or has been shut down");
		}
		HBaseSailConnection conn;
		try {
			conn = connectionFactory.createConnection(this);
		} catch (IOException ioe) {
			throw new SailException(ioe);
		}
		conn.setTrackResultSize(trackResultSize);
		conn.setTrackResultTime(trackResultTime);
		return conn;
	}

	void connectionOpened(HBaseSailConnection conn) {
		connInfos.put(conn, conn);
	}

	void connectionClosed(HBaseSailConnection conn) {
		connInfos.invalidate(conn);
	}

	@Override
    public ValueFactory getValueFactory() {
		if (valueFactory == null) {
			throw new IllegalStateException("Sail is not initialized");
		}
		return valueFactory;
    }

    @Override
    public List<IsolationLevel> getSupportedIsolationLevels() {
        return Collections.singletonList((IsolationLevel) IsolationLevels.NONE); //limited by HBase's capabilities
    }

    @Override
    public IsolationLevel getDefaultIsolationLevel() {
        return IsolationLevels.NONE;
    }

	void addQueryString(MutableBindingSet bs, String queryString) {
		bs.setBinding(HBaseSailConnection.SOURCE_STRING_BINDING, getValueFactory().createLiteral(queryString));
	}
}
