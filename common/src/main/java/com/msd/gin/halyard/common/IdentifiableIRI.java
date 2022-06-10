package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.URIUtil;

public final class IdentifiableIRI implements IRI, Identifiable, SerializableValue {
	private static final long serialVersionUID = 8055405742401584331L;
	private final String iri;
	private final RDFFactory rdfFactory;
	private int localNameIdx = -1;
	private Identifier id;
	private ByteBuffer ser;

	IdentifiableIRI(String iri, RDFFactory valueIO) {
		if (iri.indexOf(':') == -1) {
			throw new IllegalArgumentException(String.format("Not a valid (absolute) IRI: %s", iri));
		}
		this.iri = Objects.requireNonNull(iri);
		this.rdfFactory = Objects.requireNonNull(valueIO);
	}

	IdentifiableIRI(String namespace, String localName, RDFFactory valueIO) {
		this(Objects.requireNonNull(namespace, "Namespace is null") + Objects.requireNonNull(localName, "Local name is null"), valueIO);
		localNameIdx = namespace.length();
	}

	@Override
	public String getNamespace() {
		if (localNameIdx < 0) {
			localNameIdx = URIUtil.getLocalNameIndex(iri);
		}
		return iri.substring(0, localNameIdx);
	}

	@Override
	public String getLocalName() {
		if (localNameIdx < 0) {
			localNameIdx = URIUtil.getLocalNameIndex(iri);
		}
		return iri.substring(localNameIdx);
	}

	@Override
	public final String stringValue() {
		return iri;
	}

	@Override
	public final String toString() {
		return iri;
	}

	@Override
	public Identifier getId() {
		if (id == null) {
			id = rdfFactory.wellKnownId(this);
			if (id == null) {
				id = rdfFactory.id(this, getSerializedForm());
			}
		}
		return id;
	}

	@Override
	public boolean equals(Object o) {
		return this == o || o instanceof IRI
				&& iri.equals(((IRI) o).stringValue());
	}

	@Override
	public int hashCode() {
		return iri.hashCode();
	}

	@Override
	public void setId(Identifier id) {
		this.id = id;
	}

	@Override
	public ByteBuffer getSerializedForm() {
		if (ser == null) {
			ser = rdfFactory.getSerializedForm(this);
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		ByteBuffer serBuf = getSerializedForm();
		byte[] b = new byte[serBuf.remaining()];
		serBuf.get(b);
		return new SerializedValue(b, rdfFactory.streamReader);
	}
}
