package com.msd.gin.halyard.spin;

import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.sail.Sail;

import com.msd.gin.halyard.algebra.evaluation.TupleFunctionContext;

public interface SpinSail extends Sail {
	SpinParser getSpinParser();
	TupleFunctionContext.Factory getTupleFunctionContextFactory();
	FederatedServiceResolver getFederatedServiceResolver();
}
