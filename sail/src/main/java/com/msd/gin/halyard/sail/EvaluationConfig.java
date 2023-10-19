package com.msd.gin.halyard.sail;

import org.apache.hadoop.conf.Configuration;

public final class EvaluationConfig {
	public static final String TRACK_RESULT_SIZE = "halyard.evaluation.trackResultSize";
	public static final String TRACK_RESULT_TIME = "halyard.evaluation.trackResultTime";
	public static final String TRACK_BRANCH_OPERATORS_ONLY = "halyard.evaluation.trackBranchOperatorsOnly";
	public static final String QUERY_CACHE_MAX_SIZE = "hayard.evaluation.maxQueryCacheSize";
	public static final String QUERY_HISTORY_MAX_SIZE = "hayard.evaluation.maxQueryHistorySize";

	public final int queryCacheSize;
	public final boolean trackResultSize;
	public final boolean trackResultTime;
	public final boolean trackBranchOperatorsOnly;
	public final int maxQueryHistorySize;

	EvaluationConfig(Configuration config) {
		queryCacheSize = config.getInt(EvaluationConfig.QUERY_CACHE_MAX_SIZE, 100);
		trackResultSize = config.getBoolean(EvaluationConfig.TRACK_RESULT_SIZE, false);
		trackResultTime = config.getBoolean(EvaluationConfig.TRACK_RESULT_TIME, false);
		trackBranchOperatorsOnly = config.getBoolean(TRACK_BRANCH_OPERATORS_ONLY, true);
		maxQueryHistorySize = config.getInt(EvaluationConfig.QUERY_HISTORY_MAX_SIZE, 10);
	}
}
