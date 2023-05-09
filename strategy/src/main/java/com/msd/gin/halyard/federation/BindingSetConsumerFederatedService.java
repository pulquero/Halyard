package com.msd.gin.halyard.federation;

import java.util.Set;
import java.util.function.Consumer;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;

public interface BindingSetConsumerFederatedService extends FederatedService {
	void select(Consumer<BindingSet> handler, Service service, Set<String> projectionVars,
			BindingSet bindings, String baseUri) throws QueryEvaluationException;
}
