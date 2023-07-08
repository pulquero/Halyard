package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.TripleTest;
import org.eclipse.rdf4j.model.Value;

public class IdentifiableTripleTest extends TripleTest {

	@Override
	protected Triple triple(Resource s, IRI p, Value o) {
		return new IdentifiableTriple(s, p, o);
	}

	@Override
	protected IRI iri(String iri) {
		return new IdentifiableIRI(iri);
	}

}
