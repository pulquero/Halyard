package com.msd.gin.halyard.model.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public enum HalyardDatatype implements CoreDatatype {
	ARRAY(HALYARD.ARRAY_TYPE),
	MAP(HALYARD.MAP_TYPE),
	TUPLE(HALYARD.TUPLE_TYPE);

	private final IRI iri;

	HalyardDatatype(IRI iri) {
		this.iri = iri;
	}

	@Override
	public IRI getIri() {
		return iri;
	}
}
