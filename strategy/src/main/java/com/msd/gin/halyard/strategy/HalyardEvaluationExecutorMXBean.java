package com.msd.gin.halyard.strategy;

public interface HalyardEvaluationExecutorMXBean {
	void setMaxQueueSize(int size);
	int getMaxQueueSize();

	void setQueuePollTimeoutMillis(int millis);
	int getQueuePollTimeoutMillis();

	void setAsyncPullAllLimit(int limit);
	int getAsyncPullAllLimit();

	float getIncomingBindingsRatePerSecond();
	float getOutgoingBindingsRatePerSecond();

	TrackingThreadPoolExecutorMXBean getThreadPoolExecutor();
}
