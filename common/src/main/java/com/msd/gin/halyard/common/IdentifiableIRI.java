package com.msd.gin.halyard.common;

import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.URIUtil;

public final class IdentifiableIRI extends IdentifiableValue implements IRI {
	private final String iri;
	private final int localNameIdx;

	IdentifiableIRI(String iri) {
		if (iri.indexOf(':') == -1) {
			throw new IllegalArgumentException(String.format("Not a valid (absolute) IRI: %s", iri));
		}
		this.iri = Objects.requireNonNull(iri);
		this.localNameIdx = URIUtil.getLocalNameIndex(iri);
	}

	IdentifiableIRI(String namespace, String localName) {
		Objects.requireNonNull(namespace, "Namespace is null");
		Objects.requireNonNull(localName, "Local name is null");
		if (namespace.indexOf(':') == -1) {
			throw new IllegalArgumentException(String.format("Not a valid (absolute) IRI: %s", namespace));
		}
		this.iri = namespace + localName;
		this.localNameIdx = namespace.length();
	}

	@Override
	public String getNamespace() {
		return iri.substring(0, localNameIdx);
	}

	@Override
	public String getLocalName() {
		return iri.substring(localNameIdx);
	}

	@Override
	public String stringValue() {
		return iri;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		ValueIdentifier thatId = getCompatibleId(o);
		if (thatId != null) {
			return getId(null).equals(thatId);
		}
		return (o instanceof IRI)
			&& this.iri.equals(((IRI)o).stringValue());
	}

	@Override
	public int hashCode() {
		return iri.hashCode();
	}

	@Override
	public String toString() {
		return iri;
	}
}
