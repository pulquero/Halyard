package com.msd.gin.halyard.strategy.aggregators;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public final class CSVCollector implements ExtendedAggregateCollector {
	private final StringBuilder concatenated = new StringBuilder(128);
	private final String separator;

	public CSVCollector(String sep) {
		this.separator = sep;
	}

	public synchronized void append(String s) {
		if (concatenated.length() > 0) {
			concatenated.append(separator);
		}
		concatenated.append(s);
	}

	@Override
	public synchronized Value getFinalValue(TripleSource ts) {
		if (concatenated.length() == 0) {
			return ts.getValueFactory().createLiteral("");
		}

		return ts.getValueFactory().createLiteral(concatenated.toString());
	}
}
