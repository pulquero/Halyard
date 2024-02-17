package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;

public class SampleAggregateFunction extends ThreadSafeAggregateFunction<SampleCollector,Value> {

	@Override
	public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, SampleCollector col, QueryValueStepEvaluator evaluationStep) {
		Optional<Value> nextValue;
		if (!col.hasSample()) {
			Value v = evaluationStep.apply(bs);
			if (v != null) {
				// try setting the first value
				if (col.setInitial(v)) {
					return;
				} else {
					// we were beaten to it by another thread
					nextValue = Optional.ofNullable(v);
				}
			} else {
				return;
			}
		} else {
			nextValue = null;
		}

		// we flip a coin to determine if we keep the current value or set a
		// new value to report.
		if (ThreadLocalRandom.current().nextBoolean()) {
			if (nextValue == null) {
				nextValue = Optional.ofNullable(evaluationStep.apply(bs));
			}
			nextValue.ifPresent(col::setSample);
		}
	}
}
