package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;

public final class MinAggregateFunction extends ThreadSafeAggregateFunction<ValueCollector<Value>,Value> {

	public MinAggregateFunction(QueryValueStepEvaluator evaluator) {
		super(evaluator);
	}

	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, ValueCollector<Value> col) {
		Value v = evaluate(bs);
		if (v != null && distinctPredicate.test(v)) {
			col.min(v);
		}
	}
}
