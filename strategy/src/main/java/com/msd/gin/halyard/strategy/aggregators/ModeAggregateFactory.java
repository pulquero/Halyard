package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.strategy.QueryValueStepEvaluator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(AggregateFunctionFactory.class)
public final class ModeAggregateFactory implements AggregateFunctionFactory {
	@Override
	public String getIri() {
		return HALYARD.MODE_FUNCTION.stringValue();
	}

	@Override
	public AggregateFunction buildFunction(Function<BindingSet, Value> evaluationStep) {
		return new ModeAggregateFunction();
	}

	@Override
	public AggregateCollector getCollector() {
		return new ModeCollector();
	}


	private static final class ModeAggregateFunction extends ThreadSafeAggregateFunction<ModeCollector,Value> {
		@Override
		public void processAggregate(BindingSet bs, Predicate<Value> distinctPredicate, ModeCollector col, QueryValueStepEvaluator evaluationStep) {
			Value v = evaluationStep.apply(bs);
			if (v != null && distinctPredicate.test(v)) {
				col.add(v);
			}
		}
	}


	private static final class ModeCollector implements ExtendedAggregateCollector {
		private final ConcurrentHashMap<Value,AtomicLong> freqTable = new ConcurrentHashMap<>();

		void add(Value l) {
			freqTable.computeIfAbsent(l, k -> new AtomicLong()).incrementAndGet();
		}

		@Override
		public Value getFinalValue(TripleSource ts) {
			Map.Entry<Value,AtomicLong> entry = freqTable.reduceEntries(50000, (e1, e2) -> {
				if (Long.compare(e1.getValue().get(), e2.getValue().get()) >= 0) {
					return e1;
				} else {
					return e2;
				}
			});
			return (entry != null) ? entry.getKey() : null;
		}
	}
}
