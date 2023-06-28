package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.common.TupleLiteral;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(AggregateFunctionFactory.class)
public final class GroupIntoTuplesAggregateFactory implements AggregateFunctionFactory {
	@Override
	public String getIri() {
		return HALYARD.GROUP_INTO_TUPLES_FUNCTION.stringValue();
	}

	@Override
	public AggregateFunction buildFunction(Function<BindingSet, Value> evaluationStep) {
		return new GroupIntoTuplesAggregateFunction(evaluationStep);
	}

	@Override
	public AggregateCollector getCollector() {
		return new TupleCollector();
	}


	public static final class GroupIntoTuplesAggregateFunction extends ThreadSafeAggregateFunction<TupleCollector,Value> {
		GroupIntoTuplesAggregateFunction(Function<BindingSet, Value> evaluator) {
			super(evaluator);
		}

		@Override
		public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, TupleCollector col) {
			Value v = evaluate(bs);
			if (distinctPredicate.test(v)) {
				col.add(v);
			}
		}
	}

	public static final class TupleCollector implements AggregateCollector {
		private final Queue<Value> values = new ConcurrentLinkedQueue<>();

		public void add(Value v) {
			values.add(v);
		}

		@Override
		public Value getFinalValue() {
			// NB: values.size() is expensive
			return new TupleLiteral(values.toArray(new Value[0]));
		}
	}
}
