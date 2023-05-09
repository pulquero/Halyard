package com.msd.gin.halyard.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.Date;

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

public final class CachingValueFactory implements ValueFactory {
	private final ValueFactory delegate;
	private final LoadingCache<String,IRI> iriCache;
	private IRI defaultContext;
	private boolean overrideContext;

	public CachingValueFactory(ValueFactory vf, int cacheSize) {
		delegate = vf;
		iriCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(CacheLoader.from(delegate::createIRI));
	}

	public void setDefaultContext(IRI defaultContext, boolean overrideContext) {
		this.defaultContext = defaultContext;
		this.overrideContext = overrideContext;
	}

	private IRI getOrCreateIRI(String v) {
		return iriCache.getUnchecked(v);
	}

	@Override
	public IRI createIRI(String iri) {
		return getOrCreateIRI(iri);
	}

	@Override
	public IRI createIRI(String namespace, String localName) {
		return getOrCreateIRI(namespace+localName);
	}

	@Override
	public BNode createBNode() {
		return delegate.createBNode();
	}

	@Override
	public BNode createBNode(String nodeID) {
		return delegate.createBNode(nodeID);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype) {
		return delegate.createLiteral(label, datatype);
	}

	@Override
	public Literal createLiteral(String label, CoreDatatype datatype) {
		return delegate.createLiteral(label, datatype);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
		return delegate.createLiteral(label, datatype, coreDatatype);
	}

	@Override
	public Literal createLiteral(String label, String language) {
		return delegate.createLiteral(label, language);
	}

	@Override
	public Literal createLiteral(String label) {
		return delegate.createLiteral(label);
	}

	@Override
	public Literal createLiteral(boolean value) {
		return delegate.createLiteral(value);
	}

	@Override
	public Literal createLiteral(byte value) {
		return delegate.createLiteral(value);
	}

	@Override
	public Literal createLiteral(short value) {
		return delegate.createLiteral(value);
	}

	@Override
	public Literal createLiteral(int value) {
		return delegate.createLiteral(value);
	}

	@Override
	public Literal createLiteral(long value) {
		return delegate.createLiteral(value);
	}

	@Override
	public Literal createLiteral(float value) {
		return delegate.createLiteral(value);
	}

	@Override
	public Literal createLiteral(double value) {
		return delegate.createLiteral(value);
	}

	@Override
	public Literal createLiteral(BigDecimal bigDecimal) {
		return delegate.createLiteral(bigDecimal);
	}

	@Override
	public Literal createLiteral(BigInteger bigInteger) {
		return delegate.createLiteral(bigInteger);
	}

	@Override
	public Literal createLiteral(TemporalAccessor value) {
		return delegate.createLiteral(value);
	}

	@Override
	public Literal createLiteral(TemporalAmount value) {
		return delegate.createLiteral(value);
	}

	@Override
	public Literal createLiteral(XMLGregorianCalendar calendar) {
		return delegate.createLiteral(calendar);
	}

	@Override
	public Literal createLiteral(Date date) {
		return delegate.createLiteral(date);
	}

	@Override
	public Triple createTriple(Resource subject, IRI predicate, Value object) {
		return delegate.createTriple(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object) {
		return createStatementInternal(subject, predicate, object, defaultContext);
	}

	@Override
	public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
		return createStatementInternal(subject, predicate, object, overrideContext || context == null ? defaultContext : context);
	}

	private Statement createStatementInternal(Resource subject, IRI predicate, Value object, Resource context) {
		if (context != null) {
			return delegate.createStatement(subject, predicate, object, context);
		} else {
			return delegate.createStatement(subject, predicate, object);
		}
	}
}