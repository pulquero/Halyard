package com.msd.gin.halyard.strategy.aggregators;

import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public final class LongCollector implements ExtendedAggregateCollector {
	private final AtomicLong v = new AtomicLong();

	public void increment() {
		v.incrementAndGet();
	}

	@Override
	public Value getFinalValue(TripleSource ts) {
		return ts.getValueFactory().createLiteral(Long.toString(v.get()), CoreDatatype.XSD.INTEGER);
	}
}
