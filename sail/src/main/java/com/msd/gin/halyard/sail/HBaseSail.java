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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.Keyspace;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.optimizers.ExtendedEvaluationStatistics;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.ServiceStatisticsProvider;
import com.msd.gin.halyard.optimizers.StatementPatternCardinalityCalculator;
import com.msd.gin.halyard.query.algebra.evaluation.CloseableTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.QueryPreparer;
import com.msd.gin.halyard.query.algebra.evaluation.federation.SailFederatedService;
import com.msd.gin.halyard.query.algebra.evaluation.function.DynamicFunctionRegistry;
import com.msd.gin.halyard.sail.connection.SailConnectionQueryPreparer;
import com.msd.gin.halyard.sail.search.SearchClient;
import com.msd.gin.halyard.spin.SpinFunctionInterpreter;
import com.msd.gin.halyard.spin.SpinMagicPropertyInterpreter;
import com.msd.gin.halyard.spin.SpinParser;
import com.msd.gin.halyard.spin.SpinParser.Input;
import com.msd.gin.halyard.spin.SpinSail;
import com.msd.gin.halyard.strategy.StrategyConfig;
import com.msd.gin.halyard.util.MBeanDetails;
import com.msd.gin.halyard.util.MBeanManager;
import com.msd.gin.halyard.util.Version;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.CustomAggregateFunctionRegistry;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

/**
 * HBaseSail is the RDF Storage And Inference Layer (SAIL) implementation on top of Apache HBase.
 * It implements the interfaces - {@code Sail, SailConnection} and {@code FederatedServiceResolver}. Currently federated queries are
 * only supported for queries across multiple graphs in one Halyard database.
 * @author Adam Sotona (MSD)
 */
public class HBaseSail implements BindingSetConsumerSail, BindingSetPipeSail, SpinSail, HBaseSailMXBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSail.class);

    /**
	 * Ticker is a simple service interface that is notified when some data are processed. It's purpose is to notify a caller (for example MapReduce task) that the execution is
	 * still alive. Implementations must be thread-safe.
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

	public static final class QueryInfo implements Comparable<QueryInfo> {
		private final long startTimestamp = System.currentTimeMillis();
		private Long endTimestamp;
		private final String connectionId;
		private final String queryString;
		private final TupleExpr queryExpr;
		private final TupleExpr optimizedExpr;

		public QueryInfo(String connectionId, String queryString, TupleExpr queryExpr, TupleExpr optimizedExpr) {
			this.connectionId = connectionId;
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

		public String getConnectionId() {
			return connectionId;
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
		public int compareTo(QueryInfo o) {
			return Long.compare(startTimestamp, o.startTimestamp);
		}

		@Override
		public String toString() {
			return "Query: " + queryString + "\nTree:\n" + queryExpr + "\nOptimized:\n" + optimizedExpr;
		}
	}

	public static FunctionRegistry getDefaultFunctionRegistry() {
		return DynamicFunctionRegistry.getInstance();
	}

	public static CustomAggregateFunctionRegistry getDefaultAggregateFunctionRegistry() {
		return CustomAggregateFunctionRegistry.getInstance();
	}

	public static TupleFunctionRegistry getDefaultTupleFunctionRegistry() {
		return TupleFunctionRegistry.getInstance();
	}

	private static final long STATUS_CACHING_TIMEOUT = 60000l;

    private final Configuration conf; //the configuration of the HBase database
	final TableName tableName;
	final String snapshotName;
	final Path snapshotRestorePath;
	final boolean create;
	final boolean pushStrategy;
	final int splitBits;
	final int evaluationTimeoutSecs;
	private volatile boolean readOnly = true;
	private volatile long readOnlyTimestamp = 0L;
	final ElasticSettings esSettings;
	Optional<RestClientTransportWithSniffer> esTransport;
	boolean includeNamespaces = false;
	private boolean trackResultSize;
	private boolean trackResultTime;
	private boolean trackBranchOperatorsOnly;
    final Ticker ticker;
	private FederatedServiceResolver federatedServiceResolver;
	private RDFFactory rdfFactory;
	private StatementIndices stmtIndices;
	private ValueFactory valueFactory;
	private final FunctionRegistry functionRegistry = getDefaultFunctionRegistry();
	private final CustomAggregateFunctionRegistry aggregateFunctionRegistry = getDefaultAggregateFunctionRegistry();
	private final TupleFunctionRegistry tupleFunctionRegistry = getDefaultTupleFunctionRegistry();
	private final SpinParser spinParser = new SpinParser(Input.TEXT_FIRST, functionRegistry, tupleFunctionRegistry);
	private final ScanSettings scanSettings = new ScanSettings();
	final SailConnectionFactory connFactory;
	private EvaluationConfig evaluationConfig;
	private StrategyConfig strategyConfig;
	Connection hConnection;
	final boolean hConnectionIsShared; //whether a Connection is provided or we need to create our own
	Keyspace keyspace;
	volatile Optional<SearchClient> searchClient;
	QueryCache queryCache;
	private Cache<Pair<IRI, IRI>, Long> statisticsCache;
	private HalyardEvaluationStatistics statistics;
	String owner;
	private MBeanManager<HBaseSail> mbeanManager;
	private final Cache<String, HBaseSailConnection> connections = Caffeine.newBuilder().weakValues().removalListener((String id, HBaseSailConnection conn, RemovalCause cause) ->
	{
		if (cause.wasEvicted()) {
			LOGGER.warn("Unreferenced connection {} never closed", id);
		} else if (conn.isOpen()) {
			LOGGER.warn("Closing active connection {}", conn);
			conn.close();
		}
	}).build();
	private final AtomicInteger queryHistorySize = new AtomicInteger();
	private final Queue<QueryInfo> queryHistory = new ConcurrentLinkedQueue<>();

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
	 */
	private HBaseSail(@Nullable Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker,
			SailConnectionFactory connFactory) {
		this.hConnection = conn;
		this.hConnectionIsShared = (conn != null);
		this.conf = Objects.requireNonNull(config);
		this.tableName = TableName.valueOf(tableName);
		this.create = create;
		this.splitBits = splitBits;
		this.snapshotName = null;
		this.snapshotRestorePath = null;
		this.pushStrategy = pushStrategy;
		this.evaluationTimeoutSecs = evaluationTimeout;
		this.esSettings = ElasticSettings.merge(config, elasticSettings);
		this.ticker = ticker;
		this.connFactory = connFactory;
		initSettings();
	}

	public HBaseSail(Configuration config, String snapshotName, String snapshotRestorePath, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings) {
		this(config, snapshotName, snapshotRestorePath, pushStrategy, evaluationTimeout, elasticSettings, null, HBaseSailConnection.Factory.INSTANCE);
	}

	/**
	 * Construct HBaseSail for a snapshot.
	 * 
	 * @param config
	 * @param snapshotName
	 * @param snapshotRestorePath
	 * @param pushStrategy
	 * @param evaluationTimeout
	 * @param elasticSettings
	 * @param ticker
	 * @param connFactory
	 */
	public HBaseSail(Configuration config, String snapshotName, String snapshotRestorePath, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker, SailConnectionFactory connFactory) {
		this.hConnection = null;
		this.hConnectionIsShared = false;
		this.conf = Objects.requireNonNull(config);
		this.tableName = null;
		this.create = false;
		this.splitBits = -1;
		this.snapshotName = snapshotName;
		this.snapshotRestorePath = new Path(snapshotRestorePath);
		this.pushStrategy = pushStrategy;
		this.evaluationTimeoutSecs = evaluationTimeout;
		this.esSettings = ElasticSettings.merge(config, elasticSettings);
		this.ticker = ticker;
		this.connFactory = connFactory;
		initSettings();
	}

	public HBaseSail(@Nonnull Connection conn, Configuration config, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker) {
		this(conn, config, tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

	public HBaseSail(@Nonnull Connection conn, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker) {
		this(conn, conn.getConfiguration(), tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, ticker, HBaseSailConnection.Factory.INSTANCE);
	}

	HBaseSail(@Nonnull Connection conn, String tableName, boolean create, int splitBits, boolean pushStrategy, int evaluationTimeout, ElasticSettings elasticSettings, Ticker ticker, SailConnectionFactory connFactory) {
		this(conn, conn.getConfiguration(), tableName, create, splitBits, pushStrategy, evaluationTimeout, elasticSettings, ticker, connFactory);
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

	private void initSettings() {
		evaluationConfig = new EvaluationConfig(conf);
		strategyConfig = new StrategyConfig(conf);
		trackResultSize = evaluationConfig.trackResultSize;
		trackResultTime = evaluationConfig.trackResultTime;
		trackBranchOperatorsOnly = evaluationConfig.trackBranchOperatorsOnly;
		queryCache = new QueryCache(evaluationConfig.queryCacheSize);
		statisticsCache = HalyardStatsBasedStatementPatternCardinalityCalculator.newStatisticsCache();
	}

	@Override
	public String getVersion() {
		return Version.getVersionString();
	}

	@Override
	public String getTableName() {
		return (tableName != null) ? tableName.getNameWithNamespaceInclAsString() : null;
	}

	@Override
	public String getSnapshotName() {
		return snapshotName;
	}

	@Override
	public boolean isPushStrategyEnabled() {
		return pushStrategy;
	}

	@Override
	public int getEvaluationTimeout() {
		return evaluationTimeoutSecs;
	}

	public EvaluationConfig getEvaluationConfig() {
		return evaluationConfig;
	}

	public StrategyConfig getStrategyConfig() {
		return strategyConfig;
	}

	@Override
	public ElasticSettings getSearchSettings() {
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
		return (int) connections.estimatedSize();
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
	public boolean isTrackBranchOperatorsOnly() {
		return trackBranchOperatorsOnly;
	}

	@Override
	public void setTrackBranchOperatorsOnly(boolean f) {
		trackBranchOperatorsOnly = f;
	}

	@Override
	public QueryInfo[] getRecentQueries() {
		if (evaluationConfig.maxQueryHistorySize > 0) {
			List<QueryInfo> temp = new ArrayList<>(evaluationConfig.maxQueryHistorySize);
			for (QueryInfo qi : queryHistory) {
				temp.add(qi);
				if (temp.size() == evaluationConfig.maxQueryHistorySize) {
					break;
				}
			}
			QueryInfo[] result = temp.toArray(new QueryInfo[temp.size()]);
			Arrays.sort(result);
			return result;
		} else {
			return new QueryInfo[0];
		}
	}

	@Override
	public void killConnection(String id) {
		HBaseSailConnection conn = connections.getIfPresent(id);
		if (conn != null) {
			conn.close();
		}
	}

	@Override
	public void clearQueryCache() {
		queryCache.clear();
	}

	@Override
	public void clearStatisticsCache() {
		statisticsCache.invalidateAll();
	}

	@Override
	public List<String> getSearchNodes() {
		return esTransport.map(t -> t.restClient().getNodes().stream().map(n -> n.getHost().toString()).collect(Collectors.toList())).orElse(null);
	}

	@Override
	public org.apache.http.pool.PoolStats getSearchConnectionPoolStats() {
		return esTransport.map(t -> t.connectionManager().getTotalStats()).orElse(null);
	}

	public HalyardEvaluationStatistics getStatistics() {
		return statistics;
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

	QueryInfo trackQuery(HBaseSailConnection conn, String sourceString, TupleExpr rawExpr, TupleExpr optimizedExpr) {
		QueryInfo query = new QueryInfo(conn.getId(), sourceString, rawExpr, optimizedExpr);
		queryHistory.add(query);
		if (queryHistorySize.incrementAndGet() > evaluationConfig.maxQueryHistorySize) {
			queryHistory.remove();
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

	private HalyardEvaluationStatistics newStatistics() {
		if (keyspace == null) {
			throw new IllegalStateException("Sail is not initialized");
		}
		StatementPatternCardinalityCalculator.Factory spcalcFactory = () -> new HalyardStatsBasedStatementPatternCardinalityCalculator(new HBaseTripleSource(keyspace.getConnection(), valueFactory, stmtIndices, evaluationTimeoutSecs, null),
				rdfFactory, statisticsCache);
		ServiceStatisticsProvider srvStatsProvider = new ServiceStatisticsProvider() {
			final Map<String, Optional<ExtendedEvaluationStatistics>> serviceToStats = new ConcurrentHashMap<>();

			@Override
			public Optional<ExtendedEvaluationStatistics> getStatisticsForService(String serviceUrl) {
				return serviceToStats.computeIfAbsent(serviceUrl, (service) -> {
					FederatedService fedServ = federatedServiceResolver.getService(service);
					if (fedServ instanceof SailFederatedService) {
						Sail sail = ((SailFederatedService) fedServ).getSail();
						if (sail instanceof HBaseSail) {
							// need to initialize the federated service to be able to access its statistics
							if (!fedServ.isInitialized()) {
								fedServ.initialize();
							}
							return Optional.of(((HBaseSail) sail).newStatistics());
						}
					}
					return Optional.empty();
				});
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
					hConnection = HalyardTableUtils.getConnection(conf);
				}
				if (!HalyardTableUtils.tableExists(hConnection, tableName)) {
					if (create) {
						HalyardTableUtils.createTable(hConnection, tableName, splitBits).close();
					} else {
						throw new SailException(String.format("Table does not exist: %s", tableName));
					}
				}
			}

			keyspace = HalyardTableUtils.getKeyspace(conf, hConnection, tableName, snapshotName, snapshotRestorePath);
			try (KeyspaceConnection keyspaceConn = keyspace.getConnection()) {
				rdfFactory = RDFFactory.create(keyspaceConn);
			}
		} catch (IOException e) {
			throw new SailException(e);
		}
		stmtIndices = new StatementIndices(conf, rdfFactory);
		valueFactory = new IdValueFactory(rdfFactory);

		if (federatedServiceResolver == null) {
			federatedServiceResolver = new HBaseFederatedServiceResolver(hConnection, conf, tableName != null ? tableName.getNameAsString() : null, pushStrategy, evaluationTimeoutSecs, ticker);
		}

		statistics = newStatistics();

		SpinFunctionInterpreter.registerSpinParsingFunctions(spinParser, functionRegistry, pushStrategy ? tupleFunctionRegistry : TupleFunctionRegistry.getInstance());
		SpinMagicPropertyInterpreter.registerSpinParsingTupleFunctions(spinParser, tupleFunctionRegistry);

		if (esSettings != null) {
			try {
				esTransport = Optional.of(esSettings.createTransport());
			} catch (IOException | GeneralSecurityException e) {
				throw new SailException(e);
			}
		} else {
			esTransport = Optional.empty();
		}

		mbeanManager = new MBeanManager<>() {
			@Override
			protected List<MBeanDetails> mbeans(HBaseSail sail) {
				Map<String, String> attrs = new LinkedHashMap<>();
				attrs.putAll(getConnectionAttributes(owner));
				attrs.put("federatedServiceResolver", MBeanManager.getId(federatedServiceResolver));
				return Collections.singletonList(new MBeanDetails(sail, HBaseSailMXBean.class, attrs));
			}
		};
		mbeanManager.register(this);

		if (includeNamespaces) {
			try (HBaseSailConnection conn = getConnection()) {
				conn.addNamespaces();
			}
		}
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

	public Configuration getConfiguration() {
		return conf;
	}

	public FunctionRegistry getFunctionRegistry() {
		return functionRegistry;
	}

	public CustomAggregateFunctionRegistry getAggregateFunctionRegistry() {
		return aggregateFunctionRegistry;
	}

	@Override
	public TupleFunctionRegistry getTupleFunctionRegistry() {
		return tupleFunctionRegistry;
	}

	@Override
	public FederatedServiceResolver getFederatedServiceResolver() {
		return federatedServiceResolver;
	}

	@Override
	public SpinParser getSpinParser() {
		return spinParser;
	}

	@Override
	public CloseableTripleSource newTripleSource() {
		try {
			return createTripleSource(keyspace.getConnection(), true);
		} catch (IOException ioe) {
			throw new QueryEvaluationException(ioe);
		}
	}

	HBaseTripleSource createTripleSource(KeyspaceConnection keyspaceConn, boolean includeInferred) {
		return createTripleSource(keyspaceConn, includeInferred, StatementIndices.NO_PARTITIONING);
	}

	HBaseTripleSource createTripleSource(KeyspaceConnection keyspaceConn, boolean includeInferred, int forkIndex) {
		QueryPreparer.Factory qpFactory = () -> new SailConnectionQueryPreparer(getConnection(), includeInferred, getValueFactory());
		return getSearchClient().<HBaseTripleSource>map(sc -> new HBaseSearchTripleSource(keyspaceConn, getValueFactory(), getStatementIndices(), evaluationTimeoutSecs, qpFactory, getScanSettings(), sc, ticker, forkIndex))
				.orElseGet(() -> new HBaseTripleSource(keyspaceConn, getValueFactory(), getStatementIndices(), evaluationTimeoutSecs, qpFactory, getScanSettings(), ticker, forkIndex));
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

	private Optional<SearchClient> getSearchClient() {
		Optional<SearchClient> localRef = searchClient;
		if (localRef == null) {
			synchronized (this) {
				localRef = searchClient;
				if (localRef == null) {
					localRef = esTransport.map(transport -> new SearchClient(new ElasticsearchClient(transport), esSettings.indexName));
					searchClient = localRef;
				}
			}
		}
		return localRef;
	}

	@Override
	public void shutDown() throws SailException {
		connections.invalidateAll();

		if (mbeanManager != null) {
			mbeanManager.unregister();
			mbeanManager = null;
		}

		if (esTransport != null) {
			esTransport.ifPresent(transport -> {
				try {
					transport.close();
				} catch (IOException ignore) {
				}
			});
			esTransport = null;
		}
		if (federatedServiceResolver instanceof AbstractFederatedServiceResolver) {
			((AbstractFederatedServiceResolver) federatedServiceResolver).shutDown();
			federatedServiceResolver = null;
		}
		if (!hConnectionIsShared) {

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
			long lastCheckTimestamp = readOnlyTimestamp;
			if ((lastCheckTimestamp == 0) || (time > lastCheckTimestamp + STATUS_CACHING_TIMEOUT)) {
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
		if (!isConnectable()) {
			throw new IllegalStateException("Sail is not initialized or has been shut down");
		}
		try {
			return connectionFactory.createConnection(this);
		} catch (IOException ioe) {
			throw new SailException(ioe);
		}
	}

	private boolean isConnectable() {
		return (keyspace != null) && (rdfFactory != null) && (stmtIndices != null);
	}

	void connectionOpened(HBaseSailConnection conn) {
		connections.put(conn.getId(), conn);
	}

	void connectionClosed(HBaseSailConnection conn) {
		connections.invalidate(conn.getId());
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
}
