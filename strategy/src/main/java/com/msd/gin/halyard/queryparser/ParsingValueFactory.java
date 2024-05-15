package com.msd.gin.halyard.queryparser;

import com.msd.gin.halyard.common.ValueIO;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Date;

import javax.xml.datatype.DatatypeConstants;
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
import org.eclipse.rdf4j.model.vocabulary.XSD;


public final class ParsingValueFactory implements ValueFactory {
	private final ValueFactory vf;

	public ParsingValueFactory(ValueFactory vf) {
		this.vf = vf;
	}

	@Override
	public IRI createIRI(String iri) {
		return vf.createIRI(iri);
	}

	@Override
	public IRI createIRI(String namespace, String localName) {
		return vf.createIRI(namespace, localName);
	}

	@Override
	public BNode createBNode() {
		return vf.createBNode();
	}

	@Override
	public BNode createBNode(String nodeID) {
		return vf.createBNode(nodeID);
	}

	@Override
	public Literal createLiteral(String label) {
		return vf.createLiteral(label);
	}

	@Override
	public Literal createLiteral(String label, String language) {
		return vf.createLiteral(label, language);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype) {
		if (XSD.DATETIME.equals(datatype)) {
			XMLGregorianCalendar c = ValueIO.parseCalendar(label);
			if (DatatypeConstants.DATETIME.equals(c.getXMLSchemaType())) {
				return vf.createLiteral(c);
			}
		} else if (XSD.DATE.equals(datatype)) {
				XMLGregorianCalendar c = ValueIO.parseCalendar(label);
				if (DatatypeConstants.DATE.equals(c.getXMLSchemaType())) {
					return vf.createLiteral(c);
				}
		} else if (XSD.TIME.equals(datatype)) {
			XMLGregorianCalendar c = ValueIO.parseCalendar(label);
			if (DatatypeConstants.TIME.equals(c.getXMLSchemaType())) {
				return vf.createLiteral(c);
			}
		} else if (XSD.INTEGER.equals(datatype)) {
			try {
				return vf.createLiteral(new BigInteger(label));
			} catch (NumberFormatException nfe) {
				// continue
			}
		} else if (XSD.DECIMAL.equals(datatype)) {
			try {
				return vf.createLiteral(new BigDecimal(label));
			} catch (NumberFormatException nfe) {
				// continue
			}
		} else if (XSD.BOOLEAN.equals(datatype)) {
			try {
				return vf.createLiteral(ValueIO.parseBoolean(label));
			} catch (IllegalArgumentException nfe) {
				// continue
			}
		}
		return vf.createLiteral(label, datatype);
	}

	@Override
	public Literal createLiteral(String label, CoreDatatype datatype) {
		return vf.createLiteral(label, datatype);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
		return vf.createLiteral(label, datatype, coreDatatype);
	}

	@Override
	public Literal createLiteral(boolean value) {
		return vf.createLiteral(value);
	}

	@Override
	public Literal createLiteral(byte value) {
		return vf.createLiteral(value);
	}

	@Override
	public Literal createLiteral(short value) {
		return vf.createLiteral(value);
	}

	@Override
	public Literal createLiteral(int value) {
		return vf.createLiteral(value);
	}

	@Override
	public Literal createLiteral(long value) {
		return vf.createLiteral(value);
	}

	@Override
	public Literal createLiteral(float value) {
		return vf.createLiteral(value);
	}

	@Override
	public Literal createLiteral(double value) {
		return vf.createLiteral(value);
	}

	@Override
	public Literal createLiteral(BigDecimal bigDecimal) {
		return vf.createLiteral(bigDecimal);
	}

	@Override
	public Literal createLiteral(BigInteger bigInteger) {
		return vf.createLiteral(bigInteger);
	}

	@Override
	public Literal createLiteral(TemporalAccessor value) {
		return vf.createLiteral(value);
	}

	@Override
	public Literal createLiteral(TemporalAmount value) {
		return vf.createLiteral(value);
	}

	@Override
	public Literal createLiteral(XMLGregorianCalendar calendar) {
		return vf.createLiteral(calendar);
	}

	@Override
	public Literal createLiteral(Date date) {
		return vf.createLiteral(date);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object) {
		return vf.createStatement(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
		return vf.createStatement(subject, predicate, object, context);
	}

	@Override
	public Triple createTriple(Resource subject, IRI predicate, Value object) {
		return vf.createTriple(subject, predicate, object);
	}
}