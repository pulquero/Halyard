package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;

public final class MaxAggregateFunction extends ThreadSafeAggregateFunction<ValueCollector<Value>,Value> {

	public MaxAggregateFunction(QueryValueStepEvaluator evaluator) {
		super(evaluator);
	}

	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, ValueCollector<Value> col) {
		Value v = evaluate(bs);
		if (v != null && distinctPredicate.test(v)) {
			col.max(v);
		}
	}
}
