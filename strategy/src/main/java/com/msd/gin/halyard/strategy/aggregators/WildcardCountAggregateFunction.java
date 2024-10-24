package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.function.Predicate;

import org.eclipse.rdf4j.query.BindingSet;

public final class WildcardCountAggregateFunction extends ThreadSafeAggregateFunction<LongCollector,BindingSet> {

	@Override
	public void processAggregate(BindingSet bs, Predicate<BindingSet> distinctPredicate, LongCollector col, QueryValueStepEvaluator evaluationStep) {
		// for a wildcarded count we need to filter on
		// bindingsets rather than individual values.
		if (bs.size() > 0 && distinctPredicate.test(bs)) {
			col.increment();
		}
	}
}
