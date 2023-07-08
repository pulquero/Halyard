package com.msd.gin.halyard.common;

import java.util.Optional;

import org.eclipse.rdf4j.model.Literal;

/**
 * Base class for literals with complex label representations.
 */
public abstract class AbstractDataLiteral extends AbstractIdentifiableLiteral {

	private static final long serialVersionUID = -2534995635642004751L;

	private int hashCode;

	@Override
	public final Optional<String> getLanguage() {
		return Optional.empty();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		ValueIdentifier thatId = getCompatibleId(o);
		if (thatId != null) {
			return getId(null).equals(thatId);
		}
		if (o instanceof Literal) {
			Literal that = (Literal) o;
			return this.getLabel().equals(that.getLabel())
					&& this.getDatatype().equals(that.getDatatype());
		} else {
			return false;
		}
	}

	@Override
	public final int hashCode() {
		// since converting to a label can be quite expensive for these types of literals, we cache the hash code.
		if (hashCode == 0) {
			hashCode = getLabel().hashCode();
		}
		return hashCode;
	}
}
