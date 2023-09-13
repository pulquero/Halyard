package com.msd.gin.halyard.strategy.aggregators;

import java.util.function.Function;
import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;

final class ValuesAggregateFunction extends ThreadSafeAggregateFunction<ValuesCollector,Value> {
	ValuesAggregateFunction(Function<BindingSet, Value> evaluator) {
		super(evaluator);
	}

	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, ValuesCollector col) {
		Value v = evaluate(bs);
		if (v != null && distinctPredicate.test(v)) {
			col.add(v);
		}
	}
}