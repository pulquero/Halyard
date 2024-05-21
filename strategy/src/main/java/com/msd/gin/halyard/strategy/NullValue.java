package com.msd.gin.halyard.strategy;

import org.eclipse.rdf4j.model.Value;

final class NullValue implements Value {
	private static final long serialVersionUID = 9083268567256840701L;

	static final NullValue INSTANCE = new NullValue();

	private NullValue() {}

	@Override
	public String stringValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean equals(Object o) {
		return (o == INSTANCE);
	}

	@Override
	public int hashCode() {
		return 0;
	}
}
