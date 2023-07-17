package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.UUID;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

@ThreadSafe
public class IdValueFactory implements ValueFactory, Serializable {
	private static final long serialVersionUID = 5500427177071598798L;
	private final Literal TRUE;
	private final Literal FALSE;
	private transient RDFFactory rdfFactory;

	public IdValueFactory(@Nullable RDFFactory rdfFactory) {
		this.TRUE = new BooleanLiteral(true);
		this.FALSE = new BooleanLiteral(false);
		this.rdfFactory = rdfFactory;
	}

	@Override
	public IRI createIRI(String iriString) {
		IRI iri = null;
		if (rdfFactory != null) {
			iri = rdfFactory.getWellKnownIRI(iriString);
		}
		if (iri == null) {
			iri = new IdentifiableIRI(iriString);
		}
		return iri;
	}

	@Override
	public IRI createIRI(String namespace, String localName) {
		return new IdentifiableIRI(namespace, localName);
	}

	@Override
	public BNode createBNode() {
		return new IdentifiableBNode(UUID.randomUUID().toString());
	}

	@Override
	public BNode createBNode(String nodeID) {
		return new IdentifiableBNode(nodeID);
	}

	@Override
	public Literal createLiteral(String value) {
		return new IdentifiableLiteral(value);
	}

	@Override
	public Literal createLiteral(String value, String language) {
		return new IdentifiableLiteral(value, language);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype) {
		if (HALYARD.ARRAY_TYPE.equals(datatype)) {
			return new ArrayLiteral(label);
		} else if (HALYARD.MAP_TYPE.equals(datatype)) {
			return new MapLiteral(label);
		} else {
			return new IdentifiableLiteral(label, datatype);
		}
	}

	@Override
	public Literal createLiteral(String label, CoreDatatype datatype) {
		return new IdentifiableLiteral(label, datatype);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
		if (CoreDatatype.NONE == coreDatatype) {
			if (HALYARD.ARRAY_TYPE.equals(datatype)) {
				return new ArrayLiteral(label);
			} else if (HALYARD.MAP_TYPE.equals(datatype)) {
				return new MapLiteral(label);
			}
		}
		return new IdentifiableLiteral(label, datatype, coreDatatype);
	}

	@Override
	public Literal createLiteral(boolean b) {
		return b ? TRUE : FALSE;
	}

	@Override
	public Literal createLiteral(byte value) {
		return new IntLiteral(value, CoreDatatype.XSD.BYTE);
	}

	@Override
	public Literal createLiteral(short value) {
		return new IntLiteral(value, CoreDatatype.XSD.SHORT);
	}

	@Override
	public Literal createLiteral(int value) {
		return new IntLiteral(value, CoreDatatype.XSD.INT);
	}

	@Override
	public Literal createLiteral(long value) {
		return new IdentifiableLiteral(Long.toString(value), CoreDatatype.XSD.LONG);
	}

	@Override
	public Literal createLiteral(float value) {
		return new IdentifiableLiteral(IdentifiableLiteral.toString(value), CoreDatatype.XSD.FLOAT);
	}

	@Override
	public Literal createLiteral(double value) {
		return new IdentifiableLiteral(IdentifiableLiteral.toString(value), CoreDatatype.XSD.DOUBLE);
	}

	@Override
	public Literal createLiteral(BigDecimal value) {
		return new IdentifiableLiteral(value.toPlainString(), CoreDatatype.XSD.DECIMAL);
	}

	@Override
	public Literal createLiteral(BigInteger value) {
		return new IdentifiableLiteral(value.toString(), CoreDatatype.XSD.INTEGER);
	}

	@Override
	public Literal createLiteral(XMLGregorianCalendar calendar) {
		return new IdentifiableLiteral(calendar.toXMLFormat(), XMLDatatypeUtil.qnameToCoreDatatype(calendar.getXMLSchemaType()));
	}

	@Override
	public Literal createLiteral(Date date) {
		GregorianCalendar c = new GregorianCalendar();
		c.setTime(date);
		return createLiteral(ValueIO.DATATYPE_FACTORY.newXMLGregorianCalendar(c));
	}

	@Override
	public Literal createLiteral(TemporalAccessor value) {
		return new IdentifiableLiteralWrapper(SimpleValueFactory.getInstance().createLiteral(value));
	}

	@Override
	public Literal createLiteral(TemporalAmount value) {
		return new IdentifiableLiteralWrapper(SimpleValueFactory.getInstance().createLiteral(value));
	}

	@Override
	public Triple createTriple(Resource subject, IRI predicate, Value object) {
		return new IdentifiableTriple(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object) {
		return SimpleValueFactory.getInstance().createStatement(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
		return SimpleValueFactory.getInstance().createStatement(subject, predicate, object, context);
	}
}
