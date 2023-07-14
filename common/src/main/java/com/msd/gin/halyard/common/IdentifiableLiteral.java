package com.msd.gin.halyard.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Optional;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public final class IdentifiableLiteral extends IdentifiableValue implements Literal {
	IdentifiableLiteral(ByteArray ser, RDFFactory rdfFactory) {
		super(ser, rdfFactory);
	}

	IdentifiableLiteral(Literal l) {
		super(l);
	}

	IdentifiableLiteral(String label) {
		super(MATERIALIZED_VALUE_FACTORY.createLiteral(label));
	}

	IdentifiableLiteral(String label, IRI datatype) {
		super(MATERIALIZED_VALUE_FACTORY.createLiteral(label, datatype));
	}

	IdentifiableLiteral(String label, CoreDatatype coreDatatype) {
		super(MATERIALIZED_VALUE_FACTORY.createLiteral(label, coreDatatype));
	}

	IdentifiableLiteral(String label, String lang) {
		super(MATERIALIZED_VALUE_FACTORY.createLiteral(label, lang));
	}

	private Literal getLiteral() {
		return (Literal) getValue();
	}

	@Override
	public String getLabel() {
		return getLiteral().getLabel();
	}

	@Override
	public Optional<String> getLanguage() {
		return getLiteral().getLanguage();
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return getLiteral().getCoreDatatype();
	}

	@Override
	public IRI getDatatype() {
		return getLiteral().getDatatype();
	}

	@Override
	public byte byteValue() {
		return getLiteral().byteValue();
	}

	@Override
	public short shortValue() {
		return getLiteral().shortValue();
	}

	@Override
	public int intValue() {
		return getLiteral().intValue();
	}

	@Override
	public long longValue() {
		return getLiteral().longValue();
	}

	@Override
	public BigInteger integerValue() {
		return getLiteral().integerValue();
	}

	@Override
	public BigDecimal decimalValue() {
		return getLiteral().decimalValue();
	}

	@Override
	public float floatValue() {
		return getLiteral().floatValue();
	}

	@Override
	public double doubleValue() {
		return getLiteral().doubleValue();
	}

	@Override
	public boolean booleanValue() {
		return getLiteral().booleanValue();
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		return getLiteral().calendarValue();
	}

	@Override
	public TemporalAccessor temporalAccessorValue() throws DateTimeException {
		return getLiteral().temporalAccessorValue();
	}

	@Override
	public TemporalAmount temporalAmountValue() throws DateTimeException {
		return getLiteral().temporalAmountValue();
	}
}
