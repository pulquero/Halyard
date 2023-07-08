package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;

import org.eclipse.rdf4j.model.Triple;

public final class IdentifiableTriple extends TripleWrapper implements IdentifiableValue {
	private static final long serialVersionUID = 228285959274911416L;
	private transient IdSer cachedIV = IdSer.NONE;

	IdentifiableTriple(Triple triple) {
		super(triple);
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		ValueIdentifier id = current.id;
		if (current.rdfFactory != rdfFactory) {
			ByteArray ser = rdfFactory.getSerializedForm(triple);
			id = rdfFactory.id(triple, ser.copyBytes());
			cachedIV = new IdSer(id, ser, rdfFactory);
		}
		return id;
	}

	@Override
	public ByteArray getSerializedForm(RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		ByteArray ser = current.ser;
		if (current.rdfFactory != rdfFactory) {
			byte[] b = rdfFactory.valueWriter.toBytes(triple);
			ValueIdentifier id = rdfFactory.id(triple, b);
			ser = new ByteArray(b);
			cachedIV = new IdSer(id, ser, rdfFactory);
		} else if (ser == null) {
			ser = rdfFactory.getSerializedForm(triple);
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
		byte[] b = ValueIO.getDefaultWriter().toBytes(triple);
		return new SerializedValue(b);
	}
}
