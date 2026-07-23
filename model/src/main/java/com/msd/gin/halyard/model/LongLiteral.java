package com.msd.gin.halyard.model;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public final class LongLiteral extends AbstractDataLiteral {
	private static final long serialVersionUID = -9213139910409144776L;

	private final long number;
	private final CoreDatatype coreDatatype;
	private String label;

	public static LongLiteral createLong(long v) {
		return new LongLiteral(v, CoreDatatype.XSD.LONG);
	}

	public static LongLiteral createInteger(long v) {
		return new LongLiteral(v, CoreDatatype.XSD.INTEGER);
	}

	private LongLiteral(long v, CoreDatatype coreDatatype) {
		this.number = v;
		this.coreDatatype = coreDatatype;
	}

	@Override
	public String getLabel() {
		String l = label;
		if (l == null) {
			l = Long.toString(number);
			label = l;
		}
		return l;
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
		return (int) number;
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
		if (o instanceof LongLiteral) {
			LongLiteral that = (LongLiteral) o;
			return this.number == that.number && this.coreDatatype == that.coreDatatype;
		} else {
			return super.equals(o);
		}
	}
}
