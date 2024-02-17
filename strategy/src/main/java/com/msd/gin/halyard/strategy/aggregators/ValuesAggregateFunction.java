package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;

final class ValuesAggregateFunction extends ThreadSafeAggregateFunction<ValuesCollector,Value> {
	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, ValuesCollector col, QueryValueStepEvaluator evaluationStep) {
		Value v = evaluationStep.apply(bs);
		if (v != null && distinctPredicate.test(v)) {
			col.add(v);
		}
	}
}