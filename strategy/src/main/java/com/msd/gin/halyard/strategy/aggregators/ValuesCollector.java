package com.msd.gin.halyard.strategy.aggregators;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;

final class ValuesCollector implements AggregateCollector {
	private final Queue<Value> values = new ConcurrentLinkedQueue<>();
	private final Function<Collection<Value>,Value> combiner;

	ValuesCollector(Function<Collection<Value>,Value> combiner) {
		this.combiner = combiner;
	}

	public void add(Value v) {
		values.add(v);
	}

	@Override
	public Value getFinalValue() {
		return combiner.apply(values);
	}
}
