package com.msd.gin.halyard.strategy.aggregators;

import java.util.function.Function;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;

public abstract class ThreadSafeAggregateFunction<T extends AggregateCollector, D> extends AggregateFunction<T,D> {
	public ThreadSafeAggregateFunction(Function<BindingSet, Value> evaluationStep) {
		super(evaluationStep);
	}
}
