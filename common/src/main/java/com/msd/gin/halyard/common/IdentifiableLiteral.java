package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;

import org.eclipse.rdf4j.model.Literal;

public final class IdentifiableLiteral extends LiteralWrapper implements IdentifiableValue {
	private static final long serialVersionUID = 4299930477670062440L;
	private transient IdSer cachedIV = IdSer.NONE;

	IdentifiableLiteral(Literal literal) {
		super(literal);
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		ValueIdentifier id = current.id;
		if (current.rdfFactory != rdfFactory) {
			ByteArray ser = rdfFactory.getSerializedForm(literal);
			id = rdfFactory.id(literal, ser.copyBytes());
			cachedIV = new IdSer(id, ser, rdfFactory);
		}
		return id;
	}

	@Override
	public ByteArray getSerializedForm(RDFFactory rdfFactory) {
		IdSer current = cachedIV;
		ByteArray ser = current.ser;
		if (current.rdfFactory != rdfFactory) {
			byte[] b = rdfFactory.valueWriter.toBytes(literal);
			ValueIdentifier id = rdfFactory.id(literal, b);
			ser = new ByteArray(b);
			cachedIV = new IdSer(id, ser, rdfFactory);
		} else if (ser == null) {
			ser = rdfFactory.getSerializedForm(literal);
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
		byte[] b = ValueIO.getDefaultWriter().toBytes(literal);
		return new SerializedValue(b);
	}
}
