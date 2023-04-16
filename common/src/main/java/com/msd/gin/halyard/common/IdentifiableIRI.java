package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.URIUtil;

public final class IdentifiableIRI implements IRI, IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = 8055405742401584331L;
	private final String iri;
	private final int localNameIdx;
	private transient org.apache.commons.lang3.tuple.Triple<ValueIdentifier,ByteBuffer,RDFFactory> cachedIV = org.apache.commons.lang3.tuple.Triple.of(null, null, null);

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
	public final String stringValue() {
		return iri;
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
	public final String toString() {
		return iri;
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		org.apache.commons.lang3.tuple.Triple<ValueIdentifier,ByteBuffer,RDFFactory> current = cachedIV;
		ValueIdentifier id = current.getLeft();
		if (current.getRight() != rdfFactory) {
			ByteBuffer ser = rdfFactory.getSerializedForm(this);
			id = rdfFactory.id(this, ser);
			current = org.apache.commons.lang3.tuple.Triple.of(id, ser.rewind(), rdfFactory);
			cachedIV = current;
		}
		return id;
	}

	@Override
	public void setId(RDFFactory rdfFactory, ValueIdentifier id) {
		cachedIV = org.apache.commons.lang3.tuple.Triple.of(id, null, rdfFactory);
	}

	@Override
	public ByteBuffer getSerializedForm(RDFFactory rdfFactory) {
		org.apache.commons.lang3.tuple.Triple<ValueIdentifier,ByteBuffer,RDFFactory> current = cachedIV;
		ByteBuffer ser = current.getMiddle();
		if (current.getRight() != rdfFactory) {
			ser = rdfFactory.getSerializedForm(this);
			ValueIdentifier id = rdfFactory.id(this, ser);
			current = org.apache.commons.lang3.tuple.Triple.of(id, ser.rewind(), rdfFactory);
			cachedIV = current;
		} else if (current.getMiddle() == null) {
			ser = rdfFactory.getSerializedForm(this);
			current = org.apache.commons.lang3.tuple.Triple.of(current.getLeft(), ser, rdfFactory);
			cachedIV = current;
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefaultWriter().toBytes(this);
		return new SerializedValue(b);
	}
}
