package com.msd.gin.halyard.strategy;

public interface HalyardEvaluationExecutorMXBean {
	void setMaxRetries(int maxRetries);
	int getMaxRetries();

	void setRetryLimit(int limit);
	int getRetryLimit();

	void setMaxQueueSize(int size);
	int getMaxQueueSize();

	void setQueuePollTimeoutMillis(int millis);
	int getQueuePollTimeoutMillis();

	void setMinTaskRate(float rate);
	float getMinTaskRate();

	float getTaskRatePerSecond();

	TrackingThreadPoolExecutorMXBean getThreadPoolExecutor();
}
