package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public abstract class AbstractIdentifiableLiteral extends IdentifiableValue implements Literal {
	@Override
	public final String stringValue() {
		return getLabel();
	}

	@Override
	public boolean booleanValue() {
		return BooleanLiteral.parseBoolean(getLabel());
	}

	@Override
	public byte byteValue() {
		return Byte.parseByte(getLabel());
	}

	@Override
	public short shortValue() {
		return Short.parseShort(getLabel());
	}

	@Override
	public int intValue() {
		return Integer.parseInt(getLabel());
	}

	@Override
	public long longValue() {
		return Long.parseLong(getLabel());
	}

	@Override
	public BigInteger integerValue() {
		return new BigInteger(getLabel());
	}

	@Override
	public BigDecimal decimalValue() {
		return new BigDecimal(getLabel());
	}

	@Override
	public float floatValue() {
		return parseFloat(getLabel());
	}

	private static float parseFloat(String label) {
		return XMLDatatypeUtil.POSITIVE_INFINITY.equals(label) ? Float.POSITIVE_INFINITY
				: XMLDatatypeUtil.NEGATIVE_INFINITY.equals(label) ? Float.NEGATIVE_INFINITY
						: XMLDatatypeUtil.NaN.equals(label) ? Float.NaN
								: Float.parseFloat(label);
	}

	static String toString(float value) {
		return value == Float.POSITIVE_INFINITY ? XMLDatatypeUtil.POSITIVE_INFINITY
				: value == Float.NEGATIVE_INFINITY ? XMLDatatypeUtil.NEGATIVE_INFINITY
						: Float.isNaN(value) ? XMLDatatypeUtil.NaN
								: Float.toString(value);
	}

	@Override
	public double doubleValue() {
		return parseDouble(getLabel());
	}

	private static double parseDouble(String label) {
		return XMLDatatypeUtil.POSITIVE_INFINITY.equals(label) ? Double.POSITIVE_INFINITY
				: XMLDatatypeUtil.NEGATIVE_INFINITY.equals(label) ? Double.NEGATIVE_INFINITY
						: XMLDatatypeUtil.NaN.equals(label) ? Double.NaN
								: Double.parseDouble(label);
	}

	static String toString(double value) {
		return value == Double.POSITIVE_INFINITY ? XMLDatatypeUtil.POSITIVE_INFINITY
				: value == Double.NEGATIVE_INFINITY ? XMLDatatypeUtil.NEGATIVE_INFINITY
						: Double.isNaN(value) ? XMLDatatypeUtil.NaN
								: Double.toString(value);
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		return ValueIO.DATATYPE_FACTORY.newXMLGregorianCalendar(getLabel());
	}

	@Override
	public TemporalAccessor temporalAccessorValue() throws DateTimeException {
		return SimpleValueFactory.getInstance().createLiteral(getLabel()).temporalAccessorValue();
	}

	@Override
	public TemporalAmount temporalAmountValue() throws DateTimeException {
		return SimpleValueFactory.getInstance().createLiteral(getLabel()).temporalAmountValue();
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
			String thisLang = this.getLanguage().orElse(null);
			String thatLang = that.getLanguage().orElse(null);
			return this.getLabel().equals(that.getLabel())
					&& this.getDatatype().equals(that.getDatatype())
					&& ((thisLang == thatLang) || (thisLang != null && thisLang.equalsIgnoreCase(thatLang)));
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return getLabel().hashCode();
	}

	@Override
	public String toString() {
		String label = getLabel();
		CoreDatatype coreDatatype = getCoreDatatype();
		if (coreDatatype == CoreDatatype.RDF.LANGSTRING) {
			String lang = getLanguage().orElse(null);
			StringBuilder sb = new StringBuilder(label.length() + lang.length() + 3);
			sb.append("\"").append(label).append("\"");
			sb.append('@');
			sb.append(lang);
			return sb.toString();
		} else if (coreDatatype == CoreDatatype.XSD.STRING) {
			StringBuilder sb = new StringBuilder(label.length() + 2);
			sb.append("\"").append(label).append("\"");
			return sb.toString();
		} else {
			IRI datatype = getDatatype();
			StringBuilder sb = new StringBuilder(label.length() + datatype.stringValue().length() + 6);
			sb.append("\"").append(label).append("\"");
			sb.append("^^<").append(datatype.stringValue()).append(">");
			return sb.toString();
		}
	}
}
