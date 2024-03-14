package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.sail.HBaseSail.QueryInfo;
import com.msd.gin.halyard.sail.HBaseSail.ScanSettings;

import java.util.List;

public interface HBaseSailMXBean {
	String getVersion();
	String getTableName();
	String getSnapshotName();
	boolean isPushStrategyEnabled();
	int getEvaluationTimeout();
	ElasticSettings getSearchSettings();
	int getValueIdentifierSize();
	String getValueIdentifierAlgorithm();
	ScanSettings getScanSettings();

	boolean isTrackResultSize();
	void setTrackResultSize(boolean f);

	boolean isTrackResultTime();
	void setTrackResultTime(boolean f);

	boolean isTrackBranchOperatorsOnly();

	void setTrackBranchOperatorsOnly(boolean f);

	QueryInfo[] getRecentQueries();

	int getConnectionCount();

	void killConnection(String id);

	void clearQueryCache();

	void clearStatisticsCache();

	List<String> getSearchNodes();

	org.apache.http.pool.PoolStats getSearchConnectionPoolStats();
}
