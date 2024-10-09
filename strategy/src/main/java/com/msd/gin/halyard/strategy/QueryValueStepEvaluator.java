package com.msd.gin.halyard.strategy;

import java.util.function.Function;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryValueEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

public final class QueryValueStepEvaluator implements Function<BindingSet, Value> {
	private final QueryValueEvaluationStep step;
	private final ValueFactory vf;

	QueryValueStepEvaluator(QueryValueEvaluationStep step, ValueFactory vf) {
		this.step = step;
		this.vf = vf;
	}

	@Override
	public Value apply(BindingSet bs) {
		try {
			return step.evaluate(bs);
		} catch (ValueExprEvaluationException e) {
			return null; // treat missing or invalid expressions as null
		}
	}

	public ValueFactory getValueFactory() {
		return vf;
	}
}
