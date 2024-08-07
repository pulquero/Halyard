package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;

public final class IdentifiableTriple extends IdentifiableValue implements Triple {
	IdentifiableTriple(ValueIdentifier id, ByteArray ser, RDFFactory rdfFactory) {
		super(id, ser, rdfFactory);
	}

	IdentifiableTriple(Resource subject, IRI predicate, Value object) {
		super(MATERIALIZED_VALUE_FACTORY.createTriple(subject, predicate, object));
	}

	private Triple getTriple() {
		return (Triple) getValue();
	}

	@Override
	public Resource getSubject() {
		return getTriple().getSubject();
	}

	@Override
	public IRI getPredicate() {
		return getTriple().getPredicate();
	}

	@Override
	public Value getObject() {
		return getTriple().getObject();
	}
}
