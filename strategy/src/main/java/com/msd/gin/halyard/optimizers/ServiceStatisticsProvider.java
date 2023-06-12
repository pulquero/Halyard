package com.msd.gin.halyard.optimizers;

import java.util.Optional;

public interface ServiceStatisticsProvider {
	Optional<ExtendedEvaluationStatistics> getStatisticsForService(String serviceUrl);
}
