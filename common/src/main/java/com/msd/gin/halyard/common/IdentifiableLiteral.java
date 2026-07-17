package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.Wrapper;

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

public final class IdentifiableLiteral extends IdentifiableValue implements Literal, Wrapper<Literal> {
	private static String normalize(String l, CoreDatatype dt) {
		CoreDatatype.XSD xsd = dt.asXSDDatatypeOrNull();
		if (xsd != null) {
			switch (xsd) {
				case DATE:
				case DATETIME:
					return l.replace("+00:00", "Z");
				default:
					return l;
			}
		} else {
			return l;
		}
	}

	IdentifiableLiteral(ValueIdentifier id, ByteArray ser, RDFFactory rdfFactory) {
		super(id, ser, rdfFactory);
	}

	IdentifiableLiteral(Literal l) {
		super(l);
	}

	IdentifiableLiteral(String label) {
		super(MATERIALIZED_VALUE_FACTORY.createLiteral(label));
	}

	IdentifiableLiteral(String label, IRI datatype) {
		super(MATERIALIZED_VALUE_FACTORY.createLiteral(normalize(label, CoreDatatype.from(datatype)), datatype));
	}

	IdentifiableLiteral(String label, CoreDatatype coreDatatype) {
		super(MATERIALIZED_VALUE_FACTORY.createLiteral(normalize(label, coreDatatype), coreDatatype));
	}

	IdentifiableLiteral(String label, String lang) {
		super(MATERIALIZED_VALUE_FACTORY.createLiteral(label, lang));
	}

	@Override
	public Literal unwrap() {
		return (Literal) getValue();
	}

	@Override
	public String getLabel() {
		return unwrap().getLabel();
	}

	@Override
	public Optional<String> getLanguage() {
		return unwrap().getLanguage();
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return unwrap().getCoreDatatype();
	}

	@Override
	public IRI getDatatype() {
		return unwrap().getDatatype();
	}

	@Override
	public byte byteValue() {
		return unwrap().byteValue();
	}

	@Override
	public short shortValue() {
		return unwrap().shortValue();
	}

	@Override
	public int intValue() {
		return unwrap().intValue();
	}

	@Override
	public long longValue() {
		return unwrap().longValue();
	}

	@Override
	public BigInteger integerValue() {
		return unwrap().integerValue();
	}

	@Override
	public BigDecimal decimalValue() {
		return unwrap().decimalValue();
	}

	@Override
	public float floatValue() {
		return unwrap().floatValue();
	}

	@Override
	public double doubleValue() {
		return unwrap().doubleValue();
	}

	@Override
	public boolean booleanValue() {
		return unwrap().booleanValue();
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		return unwrap().calendarValue();
	}

	@Override
	public TemporalAccessor temporalAccessorValue() throws DateTimeException {
		return unwrap().temporalAccessorValue();
	}

	@Override
	public TemporalAmount temporalAmountValue() throws DateTimeException {
		return unwrap().temporalAmountValue();
	}
}
