package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;

public final class CountAggregateFunction extends ThreadSafeAggregateFunction<LongCollector,Value> {

	public CountAggregateFunction(QueryValueStepEvaluator evaluator) {
		super(evaluator);
	}

	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, LongCollector col) {
		Value value = evaluate(bs);
		if (value != null && distinctPredicate.test(value)) {
			col.increment();
		}
	}
}
