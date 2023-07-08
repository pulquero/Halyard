package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;

import org.eclipse.rdf4j.model.BNode;

public final class IdentifiableBNode extends BNodeWrapper implements IdentifiableValue {
	private static final long serialVersionUID = -6212507967580561560L;
	private transient IdSer cachedIV = IdSer.NONE;

	IdentifiableBNode(BNode bnode) {
		super(bnode);
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		ValueIdentifier id = current.id;
		if (current.rdfFactory != rdfFactory) {
			ByteArray ser = rdfFactory.getSerializedForm(bnode);
			id = rdfFactory.id(bnode, ser.copyBytes());
			cachedIV = new IdSer(id, ser, rdfFactory);
		}
		return id;
	}

	@Override
	public ByteArray getSerializedForm(RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		ByteArray ser = current.ser;
		if (current.rdfFactory != rdfFactory) {
			byte[] b = rdfFactory.valueWriter.toBytes(bnode);
			ValueIdentifier id = rdfFactory.id(bnode, b);
			ser = new ByteArray(b);
			cachedIV = new IdSer(id, ser, rdfFactory);
		} else if (ser == null) {
			ser = rdfFactory.getSerializedForm(bnode);
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
		byte[] b = ValueIO.getDefaultWriter().toBytes(bnode);
		return new SerializedValue(b);
	}
}
