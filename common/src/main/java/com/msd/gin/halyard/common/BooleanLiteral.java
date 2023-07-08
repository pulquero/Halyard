package com.msd.gin.halyard.common;

import java.util.Optional;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public final class BooleanLiteral extends AbstractIdentifiableLiteral {
	private static final String TRUE = "true";
	private static final int TRUE_HASH = TRUE.hashCode();
	private static final String FALSE = "false";
	private static final int FALSE_HASH = FALSE.hashCode();

	static boolean parseBoolean(String label) {
		label = label.trim();
		if (TRUE.equals(label) || "1".equals(label)) {
			return true;
		} else if (FALSE.equals(label) || "0".equals(label)) {
			return false;
		} else {
			throw new IllegalArgumentException("Malformed boolean");
		}
	}

	private final boolean value;

	public BooleanLiteral(boolean value) {
		this.value = value;
	}

	@Override
	public String getLabel() {
		return value ? TRUE : FALSE;
	}

	@Override
	public Optional<String> getLanguage() {
		return Optional.empty();
	}

	@Override
	public IRI getDatatype() {
		return XSD.BOOLEAN;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.XSD.BOOLEAN;
	}

	@Override
	public boolean booleanValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof BooleanLiteral) {
			BooleanLiteral that = (BooleanLiteral) o;
			return this.value == that.value;
		} else if (o instanceof Literal) {
			Literal that = (Literal) o;
			return this.getLabel().equals(that.getLabel())
					&& this.getDatatype().equals(that.getDatatype());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return value ? TRUE_HASH : FALSE_HASH;
	}
}
