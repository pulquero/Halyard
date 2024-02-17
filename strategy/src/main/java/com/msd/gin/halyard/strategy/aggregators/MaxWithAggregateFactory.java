package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.common.TupleLiteral;
import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.function.Function;
import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(AggregateFunctionFactory.class)
public final class MaxWithAggregateFactory implements AggregateFunctionFactory {
	@Override
	public String getIri() {
		return HALYARD.MAX_WITH_FUNCTION.stringValue();
	}

	@Override
	public AggregateFunction buildFunction(Function<BindingSet, Value> evaluationStep) {
		return new MaxWithAggregateFunction();
	}

	@Override
	public AggregateCollector getCollector() {
		return new ValueCollector<TupleLiteral>(new TupleLiteralComparator(false));
	}


	private static final class MaxWithAggregateFunction extends ThreadSafeAggregateFunction<ValueCollector<TupleLiteral>,Value> {

		@Override
		public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, ValueCollector<TupleLiteral> col, QueryValueStepEvaluator evaluationStep) {
			Value v = evaluationStep.apply(bs);
			if (TupleLiteral.isTupleLiteral(v)) {
				TupleLiteral l = TupleLiteral.asTupleLiteral(v);
				if (distinctPredicate.test(l)) {
					col.max(l);
				}
			}
		}
	}
}
