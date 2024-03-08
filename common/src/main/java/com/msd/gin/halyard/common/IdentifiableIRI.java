package com.msd.gin.halyard.common;

import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;

public final class IdentifiableIRI extends IdentifiableValue implements IRI {
	IdentifiableIRI(ByteArray ser, RDFFactory rdfFactory) {
		super(ser, rdfFactory);
	}

	IdentifiableIRI(String iri) {
		super(MATERIALIZED_VALUE_FACTORY.createIRI(Objects.requireNonNull(iri)));
	}

	IdentifiableIRI(String namespace, String localName) {
		super(MATERIALIZED_VALUE_FACTORY.createIRI(
			Objects.requireNonNull(namespace, "Namespace is null"),
			Objects.requireNonNull(localName, "Local name is null")
		));
	}

	private IRI getIRI() {
		return (IRI) getValue();
	}

	@Override
	public String getNamespace() {
		return getIRI().getNamespace();
	}

	@Override
	public String getLocalName() {
		return getIRI().getLocalName();
	}

	public boolean isWellKnown() {
		return getEncodingType() == HeaderBytes.IRI_HASH_TYPE;
	}
}
