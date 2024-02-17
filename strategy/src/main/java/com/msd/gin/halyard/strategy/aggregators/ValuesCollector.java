package com.msd.gin.halyard.strategy.aggregators;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

final class ValuesCollector implements ExtendedAggregateCollector {
	// concurrent sequence of values
	private final Queue<Value> values = new ConcurrentLinkedQueue<>();
	private final Function<Collection<Value>,Value> combiner;

	ValuesCollector(Function<Collection<Value>,Value> combiner) {
		this.combiner = combiner;
	}

	public void add(Value v) {
		values.add(v);
	}

	@Override
	public Value getFinalValue(TripleSource ts) {
		return combiner.apply(values);
	}
}
