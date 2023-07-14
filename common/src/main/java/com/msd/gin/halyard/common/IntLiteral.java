package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.AbstractLiteral;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;

public final class IntLiteral extends AbstractLiteral {
	private static final long serialVersionUID = 4017662080461402992L;

	private final String label;
	private final int number;
	private final CoreDatatype coreDatatype;

	public IntLiteral(int v, CoreDatatype coreDatatype) {
		this.label = XMLDatatypeUtil.toString(v);
		this.number = v;
		this.coreDatatype = coreDatatype;
	}

	@Override
	public String getLabel() {
		return label;
	}

	@Override
	public Optional<String> getLanguage() {
		return Optional.empty();
	}

	@Override
	public IRI getDatatype() {
		return coreDatatype.getIri();
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return coreDatatype;
	}

	@Override
	public boolean booleanValue() {
		if (number == 1) {
			return true;
		} else if (number == 0) {
			return false;
		} else {
			throw new IllegalArgumentException("Malformed value");
		}
	}

	@Override
	public byte byteValue() {
		return (byte) number;
	}

	@Override
	public short shortValue() {
		return (short) number;
	}

	@Override
	public int intValue() {
		return number;
	}

	@Override
	public long longValue() {
		return number;
	}

	@Override
	public BigInteger integerValue() {
		return BigInteger.valueOf(number);
	}

	@Override
	public BigDecimal decimalValue() {
		return BigDecimal.valueOf(number);
	}

	@Override
	public float floatValue() {
		return number;
	}

	@Override
	public double doubleValue() {
		return number;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof IntLiteral) {
			IntLiteral that = (IntLiteral) o;
			return this.number == that.number && this.coreDatatype == that.coreDatatype;
		} else if (o instanceof Literal) {
			Literal that = (Literal) o;
			return this.label.equals(that.getLabel())
					&& this.getDatatype().equals(that.getDatatype());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return label.hashCode();
	}
}
