package com.msd.gin.halyard.strategy.aggregators;

import com.msd.gin.halyard.model.TupleLiteral;

import java.util.Comparator;

import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;

final class TupleLiteralComparator implements Comparator<TupleLiteral> {
	private final ValueComparator comparator = new ValueComparator();

	public TupleLiteralComparator(boolean isStrict) {
		comparator.setStrict(isStrict);
	}

	public boolean isStrict() {
		return comparator.isStrict();
	}

	@Override
	public int compare(TupleLiteral o1, TupleLiteral o2) {
		return comparator.compare(o1.objectValue()[0], o2.objectValue()[0]);
	}
}