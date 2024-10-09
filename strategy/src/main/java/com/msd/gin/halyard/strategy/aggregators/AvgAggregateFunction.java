package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

public class AvgAggregateFunction extends ThreadSafeAggregateFunction<AvgCollector,Value> {

	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, AvgCollector col, QueryValueStepEvaluator evaluationStep) {
		if (col.hasError()) {
			// Prevent calculating the aggregate further if a type error has
			// occured.
			return;
		}

		Value v = evaluationStep.apply(bs);
		if ( v != null) {
			try {
				if (!v.isLiteral()) {
					throw new ValueExprEvaluationException("not a literal: " + v);
				}
				if (distinctPredicate.test(v)) {
					Literal nextLiteral = (Literal) v;
					col.addValue(nextLiteral, evaluationStep.getValueFactory());
					col.incrementCount();
				}
			} catch (ValueExprEvaluationException ex) {
				// we do not actually throw the exception yet, but record it and
				// stop further processing. The exception will be thrown when
				// getValue() is invoked.
				col.setError(ex);
			}
		}
	}
}
