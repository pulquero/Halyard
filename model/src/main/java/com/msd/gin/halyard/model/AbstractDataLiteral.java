package com.msd.gin.halyard.model;

import java.util.Optional;

import org.eclipse.rdf4j.model.base.AbstractLiteral;

/**
 * Base class for literals with complex label representations.
 */
public abstract class AbstractDataLiteral extends AbstractLiteral {

	private static final long serialVersionUID = -2534995635642004751L;

	private int hashCode;

	@Override
	public final Optional<String> getLanguage() {
		return Optional.empty();
	}

	@Override
	public final int hashCode() {
		// since converting to a label can be quite expensive for these types of literals, we cache the hash code.
		int hc = hashCode;
		if (hc == 0) {
			hc = super.hashCode();
			hashCode = hc;
		}
		return hc;
	}
}
