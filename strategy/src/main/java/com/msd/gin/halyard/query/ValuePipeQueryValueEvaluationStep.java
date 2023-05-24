package com.msd.gin.halyard.query;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryValueEvaluationStep;

public interface ValuePipeQueryValueEvaluationStep extends QueryValueEvaluationStep {
	/**
	 * NB: asynchronous.
	 * @param parent
	 * @param bindings
	 */
	void evaluate(ValuePipe parent, BindingSet bindings);
}
