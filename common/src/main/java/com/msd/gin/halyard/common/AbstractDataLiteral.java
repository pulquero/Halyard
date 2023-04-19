package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

public abstract class AbstractDataLiteral implements Literal {

	private static final long serialVersionUID = -2534995635642004751L;

	@Override
	public final String stringValue() {
		return getLabel();
	}

	@Override
	public final Optional<String> getLanguage() {
		return Optional.empty();
	}

	@Override
	public final byte byteValue() {
		throw new NumberFormatException("Not a number");
	}

	@Override
	public final short shortValue() {
		throw new NumberFormatException("Not a number");
	}

	@Override
	public final int intValue() {
		throw new NumberFormatException("Not a number");
	}

	@Override
	public final long longValue() {
		throw new NumberFormatException("Not a number");
	}

	@Override
	public final BigInteger integerValue() {
		throw new NumberFormatException("Not a number");
	}

	@Override
	public final BigDecimal decimalValue() {
		throw new NumberFormatException("Not a number");
	}

	@Override
	public final float floatValue() {
		throw new NumberFormatException("Not a number");
	}

	@Override
	public final double doubleValue() {
		throw new NumberFormatException("Not a number");
	}

	@Override
	public final boolean booleanValue() {
		throw new IllegalArgumentException("Not boolean");
	}

	@Override
	public final XMLGregorianCalendar calendarValue() {
		throw new IllegalArgumentException("Not a temporal value");
	}

	@Override
	public final int hashCode() {
		return getLabel().hashCode();
	}

	@Override
	public String toString() {
		String label = getLabel();
		IRI datatype = getDatatype();
		StringBuilder sb = new StringBuilder(label.length() + datatype.stringValue().length() + 6);
		sb.append("\"").append(label).append("\"");
		sb.append("^^<").append(datatype.stringValue()).append(">");
		return sb.toString();
	}
}
