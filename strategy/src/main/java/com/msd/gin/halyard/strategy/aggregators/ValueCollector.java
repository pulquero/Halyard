package com.msd.gin.halyard.strategy.aggregators;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;

public final class ValueCollector<V extends Value> implements ExtendedAggregateCollector {
	private final AtomicReference<V> vref = new AtomicReference<>();
	private final Comparator<V> comparator;

	public static ValueCollector<Value> create(boolean isStrict) {
		ValueComparator comparator = new ValueComparator();
		comparator.setStrict(isStrict);
		return new ValueCollector<>(comparator);
	}


	public ValueCollector(Comparator<V> comparator) {
		this.comparator = comparator;
	}

	public void min(V val) {
		vref.accumulateAndGet(val, (current,next) -> {
			if (current == null || comparator.compare(next, current) < 0) {
				return next;
			} else {
				return current;
			}
		});
	}

	public void max(V val) {
		vref.accumulateAndGet(val, (current,next) -> {
			if (current == null || comparator.compare(next, current) > 0) {
				return next;
			} else {
				return current;
			}
		});
	}

	@Override
	public Value getFinalValue(TripleSource ts) {
		return vref.get();
	}
}
