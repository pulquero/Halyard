package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;

public class SampleAggregateFunction extends ThreadSafeAggregateFunction<SampleCollector,Value> {

	public SampleAggregateFunction(QueryValueStepEvaluator evaluator) {
		super(evaluator);
	}

	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, SampleCollector col) {
		// we flip a coin to determine if we keep the current value or set a
		// new value to report.
		Optional<Value> newValue = null;
		if (!col.hasSample()) {
			newValue = Optional.ofNullable(evaluate(bs));
			if (newValue.isPresent() && col.setInitial(newValue.get())) {
				return;
			}
		}

		if (ThreadLocalRandom.current().nextFloat() < 0.5f) {
			if (newValue == null) {
				newValue = Optional.ofNullable(evaluate(bs));
			}
			newValue.ifPresent(col::setSample);
		}
	}
}
