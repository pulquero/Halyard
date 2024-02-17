package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.io.Serializable;
import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;

public abstract class ExtendedAggregateFunction<T extends ExtendedAggregateCollector, D> extends AggregateFunction<T,D> implements Serializable {
	protected ExtendedAggregateFunction() {
		super(null);
	}

	public abstract void processAggregate(BindingSet bindingSet, Predicate<D> distinctValue, T agv, QueryValueStepEvaluator evaluationStep)
			throws QueryEvaluationException;

	@Override
	public final void processAggregate(BindingSet bindingSet, Predicate<D> distinctValue, T agv)
			throws QueryEvaluationException {
		throw new UnsupportedOperationException();
	}

	@Override
	protected final Value evaluate(BindingSet s) throws QueryEvaluationException {
		throw new UnsupportedOperationException();
	}
}
