package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.IRITest;

public class IdentifiableIRITest extends IRITest {
    private static final RDFFactory rdfFactory = RDFFactory.create();

	@Override
	protected IRI iri(String iri) {
		return new IdentifiableIRI(iri, rdfFactory);
	}

	@Override
	protected IRI iri(String namespace, String localname) {
		return new IdentifiableIRI(namespace, localname, rdfFactory);
	}

}
