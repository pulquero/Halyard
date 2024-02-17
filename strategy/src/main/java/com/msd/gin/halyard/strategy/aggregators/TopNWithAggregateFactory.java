package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.common.TupleLiteral;
import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.kohsuke.MetaInfServices;

/**
 * halyard:topNWith(halyard:tuple(?value, ?N, ?extra1, ?extra2, ...))
 * Returns a tuple of N tuples of the form ?value ?N ?extra1 ?extra2 ...
 */
@MetaInfServices(AggregateFunctionFactory.class)
public final class TopNWithAggregateFactory implements AggregateFunctionFactory {
	@Override
	public String getIri() {
		return HALYARD.TOP_N_WITH_FUNCTION.stringValue();
	}

	@Override
	public AggregateFunction buildFunction(Function<BindingSet, Value> evaluationStep) {
		return new TopNWithAggregateFunction();
	}

	@Override
	public AggregateCollector getCollector() {
		return new TopNCollector();
	}


	public static final class TopNWithAggregateFunction extends ThreadSafeAggregateFunction<TopNCollector,Value> {
		@Override
		public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, TopNCollector col, QueryValueStepEvaluator evaluationStep) {
			Value v = evaluationStep.apply(bs);
			if (TupleLiteral.isTupleLiteral(v)) {
				TupleLiteral l = TupleLiteral.asTupleLiteral(v);
				if (distinctPredicate.test(l)) {
					col.add(l);
				}
			}
		}
	}

	private static final class TopNCollector implements ExtendedAggregateCollector {
		private final Comparator<TupleLiteral> comparator = new TupleLiteralComparator(false);
		private final PriorityBlockingQueue<TupleLiteral> topN = new PriorityBlockingQueue<>(10, comparator);
		private final AtomicInteger topNSize = new AtomicInteger();

		void add(TupleLiteral val) {
			topN.add(val);
			int n = ((Literal) val.objectValue()[1]).intValue();
			if (topNSize.incrementAndGet() > n) {
				topN.remove();
			}
		}

		@Override
		public Value getFinalValue(TripleSource ts) {
			TupleLiteral[] arr = topN.toArray(new TupleLiteral[0]);
			Arrays.sort(arr, comparator.reversed());
			return new TupleLiteral(arr);
		}
	}
}
