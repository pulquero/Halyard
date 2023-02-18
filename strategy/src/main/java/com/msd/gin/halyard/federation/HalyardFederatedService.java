package com.msd.gin.halyard.federation;

import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;

public interface HalyardFederatedService extends FederatedService {
	public abstract FederatedService createPrivateInstance();
}
