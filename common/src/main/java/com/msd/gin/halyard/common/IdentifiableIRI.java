package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.URIUtil;

public final class IdentifiableIRI implements IRI, IdentifiableValue {
	private static final long serialVersionUID = 8055405742401584331L;
	private final String iri;
	private final int localNameIdx;
	private transient IdSer cachedIV = IdSer.NONE;

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
		IdSer current = cachedIV;
		ValueIdentifier id = current.id;
		if (current.rdfFactory != rdfFactory) {
			ByteArray ser = rdfFactory.getSerializedForm(this);
			id = rdfFactory.id(this, ser.copyBytes());
			cachedIV = new IdSer(id, ser, rdfFactory);
		}
		return id;
	}

	@Override
	public ByteArray getSerializedForm(RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		ByteArray ser = current.ser;
		if (current.rdfFactory != rdfFactory) {
			byte[] b = rdfFactory.valueWriter.toBytes(this);
			ValueIdentifier id = rdfFactory.id(this, b);
			ser = new ByteArray(b);
			cachedIV = new IdSer(id, ser, rdfFactory);
		} else if (ser == null) {
			ser = rdfFactory.getSerializedForm(this);
			cachedIV = new IdSer(current.id, ser, rdfFactory);
		}
		return ser;
	}

	@Override
	public void setId(RDFFactory rdfFactory, ValueIdentifier id) {
		IdSer current = cachedIV;
		if (current.rdfFactory != rdfFactory) {
			cachedIV = new IdSer(id, null, rdfFactory);
		}
	}

	@Override
	public void setIdSer(RDFFactory rdfFactory, ValueIdentifier id, ByteArray ser) {
		IdSer current = cachedIV;
		if (current.rdfFactory != rdfFactory) {
			cachedIV = new IdSer(id, ser, rdfFactory);
		}
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefaultWriter().toBytes(this);
		return new SerializedValue(b);
	}
}
