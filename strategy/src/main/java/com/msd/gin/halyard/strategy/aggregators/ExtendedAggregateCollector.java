package com.msd.gin.halyard.strategy.aggregators;

import java.io.Serializable;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;

public interface ExtendedAggregateCollector extends AggregateCollector, Serializable {
	Value getFinalValue(TripleSource ts);

	@Override
	default Value getFinalValue() {
		throw new UnsupportedOperationException();
	}
}
