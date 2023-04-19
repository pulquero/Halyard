package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.common.TupleLiteral;
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
public final class MinWithAggregateFactory implements AggregateFunctionFactory {
	@Override
	public String getIri() {
		return HALYARD.MIN_WITH_FUNCTION.stringValue();
	}

	@Override
	public AggregateFunction buildFunction(Function<BindingSet, Value> evaluationStep) {
		return new MinWithAggregateFunction(evaluationStep);
	}

	@Override
	public AggregateCollector getCollector() {
		return new ValueCollector<TupleLiteral>(new TupleLiteralComparator(false));
	}


	public static final class MinWithAggregateFunction extends ThreadSafeAggregateFunction<ValueCollector<TupleLiteral>,Value> {
		MinWithAggregateFunction(Function<BindingSet, Value> evaluator) {
			super(evaluator);
		}

		@Override
		public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, ValueCollector<TupleLiteral> col) {
			Value v = evaluate(bs);
			if (v instanceof TupleLiteral) {
				TupleLiteral l = (TupleLiteral) v;
				if (distinctPredicate.test(l)) {
					col.min(l);
				}
			}
		}
	}
}
