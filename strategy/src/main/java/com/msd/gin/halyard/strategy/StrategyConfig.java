package com.msd.gin.halyard.strategy;

import org.apache.hadoop.conf.Configuration;

public final class StrategyConfig {

	public static final String HALYARD_EVALUATION_HASH_JOIN_LIMIT = "halyard.evaluation.hashJoin.limit";
	public static final String HALYARD_EVALUATION_HASH_JOIN_COST_RATIO = "halyard.evaluation.hashJoin.costRatio";
	public static final String HALYARD_EVALUATION_STAR_JOIN_MIN_JOINS = "halyard.evaluation.starJoin.minJoins";
	public static final String HALYARD_EVALUATION_NARY_UNION_MIN_UNIONS = "halyard.evaluation.naryUnion.minUnions";
	public static final String HALYARD_EVALUATION_MEMORY_THRESHOLD = "halyard.evaluation.collections.memoryThreshold";
	public static final String HALYARD_EVALUATION_VALUE_CACHE_SIZE = "halyard.evaluation.valueCache.size";
	public static final String HALYARD_EVALUATION_POLL_TIMEOUT_MILLIS = "halyard.evaluation.pollTimeoutMillis";
	public static final String HALYARD_EVALUATION_OFFER_TIMEOUT_MILLIS = "halyard.evaluation.offerTimeoutMillis";
	public static final String HALYARD_EVALUATION_MAX_QUEUE_SIZE = "halyard.evaluation.maxQueueSize";
	public static final String HALYARD_EVALUATION_THREADS = "halyard.evaluation.threads";
	public static final String HALYARD_EVALUATION_BINDINGS_RATE_UPDATE_MILLIS = "halyard.evaluation.bindingsRate.updateMillis";
	public static final String HALYARD_EVALUATION_BINDINGS_RATE_WINDOW_SIZE = "halyard.evaluation.bindingsRate.windowSize";
	public static final String HALYARD_EVALUATION_TRACK_RESULT_SIZE_UPDATE_INTERVAL = "halyard.evaluation.trackResultSize.updateInterval";
	public static final String HALYARD_EVALUATION_TRACK_RESULT_TIME_UPDATE_INTERVAL = "halyard.evaluation.trackResultTime.updateInterval";

	static final int DEFAULT_HASH_JOIN_LIMIT = 50000;
	static final int DEFAULT_STAR_JOIN_MIN_JOINS = 3;
	static final int DEFAULT_NARY_UNION_MIN_UNIONS = 2;
	static final int DEFAULT_MEMORY_THRESHOLD = 100000;
	static final int DEFAULT_VALUE_CACHE_SIZE = 1000;
	static final int DEFAULT_QUEUE_SIZE = 5000;
	static final int DEFAULT_THREADS = 25;
	public static final String JMX_DOMAIN = "com.msd.gin.halyard";

	final long trackResultSizeUpdateInterval;
	final long trackResultTimeUpdateInterval;
	final int starJoinMinJoins;
	final int naryUnionMinUnions;
	final int hashJoinLimit;
	final float hashJoinCostRatio;
	final int collectionMemoryThreshold;
	final int valueCacheSize;

	public StrategyConfig(Configuration conf) {
		this.trackResultSizeUpdateInterval = conf.getLong(HALYARD_EVALUATION_TRACK_RESULT_SIZE_UPDATE_INTERVAL, Long.MAX_VALUE);
		this.trackResultTimeUpdateInterval = conf.getLong(HALYARD_EVALUATION_TRACK_RESULT_TIME_UPDATE_INTERVAL, Long.MAX_VALUE);
		this.starJoinMinJoins = conf.getInt(HALYARD_EVALUATION_STAR_JOIN_MIN_JOINS, DEFAULT_STAR_JOIN_MIN_JOINS);
		this.naryUnionMinUnions = conf.getInt(HALYARD_EVALUATION_NARY_UNION_MIN_UNIONS, DEFAULT_NARY_UNION_MIN_UNIONS);
		this.hashJoinLimit = conf.getInt(HALYARD_EVALUATION_HASH_JOIN_LIMIT, DEFAULT_HASH_JOIN_LIMIT);
		this.hashJoinCostRatio = conf.getFloat(HALYARD_EVALUATION_HASH_JOIN_COST_RATIO, 2.0f);
    	this.collectionMemoryThreshold = conf.getInt(HALYARD_EVALUATION_MEMORY_THRESHOLD, DEFAULT_MEMORY_THRESHOLD);
    	this.valueCacheSize = conf.getInt(HALYARD_EVALUATION_VALUE_CACHE_SIZE, DEFAULT_VALUE_CACHE_SIZE);
	}
}
