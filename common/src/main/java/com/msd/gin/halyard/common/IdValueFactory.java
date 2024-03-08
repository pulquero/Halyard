package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.IntLiteral;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Date;
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

@ThreadSafe
public class IdValueFactory implements ValueFactory, Serializable {
	private static final long serialVersionUID = 5500427177071598798L;
	private static final ValueFactory DELEGATE_VALUE_FACTORY = IdentifiableValue.MATERIALIZED_VALUE_FACTORY;

	private final Literal TRUE;
	private final Literal FALSE;
	private transient RDFFactory rdfFactory;

	public IdValueFactory(@Nullable RDFFactory rdfFactory) {
		this.TRUE = new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(true));
		this.FALSE = new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(false));
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
		return new IdentifiableLiteral(label, datatype);
	}

	@Override
	public Literal createLiteral(String label, CoreDatatype coreDatatype) {
		return new IdentifiableLiteral(label, coreDatatype);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
		if (coreDatatype == CoreDatatype.NONE) {
			return new IdentifiableLiteral(label, datatype);
		} else {
			return new IdentifiableLiteral(label, coreDatatype);
		}
	}

	@Override
	public Literal createLiteral(boolean b) {
		return b ? TRUE : FALSE;
	}

	@Override
	public Literal createLiteral(byte value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	@Override
	public Literal createLiteral(short value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	@Override
	public Literal createLiteral(int value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	public Literal createLiteral(int value, CoreDatatype coreDatatype) {
		return new IdentifiableLiteral(new IntLiteral(value, coreDatatype));
	}

	@Override
	public Literal createLiteral(long value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	@Override
	public Literal createLiteral(float value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	@Override
	public Literal createLiteral(double value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	@Override
	public Literal createLiteral(BigDecimal value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	@Override
	public Literal createLiteral(BigInteger value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	@Override
	public Literal createLiteral(XMLGregorianCalendar calendar) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(calendar));
	}

	@Override
	public Literal createLiteral(Date date) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(date));
	}

	@Override
	public Literal createLiteral(TemporalAccessor value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	@Override
	public Literal createLiteral(TemporalAmount value) {
		return new IdentifiableLiteral(DELEGATE_VALUE_FACTORY.createLiteral(value));
	}

	@Override
	public Triple createTriple(Resource subject, IRI predicate, Value object) {
		return new IdentifiableTriple(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object) {
		return DELEGATE_VALUE_FACTORY.createStatement(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
		return DELEGATE_VALUE_FACTORY.createStatement(subject, predicate, object, context);
	}
}
