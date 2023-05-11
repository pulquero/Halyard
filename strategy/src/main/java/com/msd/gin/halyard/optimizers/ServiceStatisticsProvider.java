package com.msd.gin.halyard.optimizers;

public interface ServiceStatisticsProvider {
	ExtendedEvaluationStatistics getStatisticsForService(String serviceUrl);
}
