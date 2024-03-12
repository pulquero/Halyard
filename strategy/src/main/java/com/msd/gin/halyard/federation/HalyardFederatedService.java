package com.msd.gin.halyard.federation;

import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;

import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;

public interface HalyardFederatedService extends FederatedService {
	FederatedService createEvaluationInstance(HalyardEvaluationStrategy strategy, int forkIndex);
}
