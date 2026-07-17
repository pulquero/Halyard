package com.msd.gin.halyard.model;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public final class IntLiteral extends AbstractDataLiteral {
	private static final long serialVersionUID = 4017662080461402992L;

	private static final IntLiteral[] INTEGER_CACHE = new IntLiteral[101];

	static {
		for (int i=0; i<INTEGER_CACHE.length; i++) {
			INTEGER_CACHE[i] = new IntLiteral(i, CoreDatatype.XSD.INTEGER);
		}
	}

	private final int number;
	private final CoreDatatype coreDatatype;
	private String label;

	public static IntLiteral createByte(byte v) {
		return new IntLiteral(v, CoreDatatype.XSD.BYTE);
	}

	public static IntLiteral createShort(short v) {
		return new IntLiteral(v, CoreDatatype.XSD.SHORT);
	}

	public static IntLiteral createInt(int v) {
		return new IntLiteral(v, CoreDatatype.XSD.INT);
	}

	public static IntLiteral createInteger(int v) {
		if (v >= 0 && v < INTEGER_CACHE.length) {
			return INTEGER_CACHE[v];
		} else {
			return new IntLiteral(v, CoreDatatype.XSD.INTEGER);
		}
	}

	private IntLiteral(int v, CoreDatatype coreDatatype) {
		this.number = v;
		this.coreDatatype = coreDatatype;
	}

	@Override
	public String getLabel() {
		String l = label;
		if (l == null) {
			l = Integer.toString(number);
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
		} else {
			return super.equals(o);
		}
	}
}
