package com.msd.gin.halyard.strategy;

public interface HalyardEvaluationExecutorMXBean {
	void setMaxQueueSize(int size);
	int getMaxQueueSize();

	void setQueuePollTimeoutMillis(int millis);
	int getQueuePollTimeoutMillis();

	void setAsyncPullPushAllLimit(int limit);
	int getAsyncPullPushAllLimit();

	void setAsyncTaskQueueMaxSize(int size);
	int getAsyncTaskQueueMaxSize();

	float getIncomingBindingsRatePerSecond();
	float getOutgoingBindingsRatePerSecond();

	TrackingThreadPoolExecutorMXBean getThreadPoolExecutor();
}
