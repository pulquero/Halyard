package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Literal;

public final class IdentifiableLiteral extends LiteralWrapper implements IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = 4299930477670062440L;
	private transient org.apache.commons.lang3.tuple.Triple<ValueIdentifier,ByteBuffer,RDFFactory> cachedIV = org.apache.commons.lang3.tuple.Triple.of(null, null, null);

	IdentifiableLiteral(Literal literal) {
		super(literal);
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		org.apache.commons.lang3.tuple.Triple<ValueIdentifier,ByteBuffer,RDFFactory> current = cachedIV;
		ValueIdentifier id = current.getLeft();
		if (current.getRight() != rdfFactory) {
			ByteBuffer ser = rdfFactory.getSerializedForm(literal);
			id = rdfFactory.id(literal, ser);
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
			ser = rdfFactory.getSerializedForm(literal);
			ValueIdentifier id = rdfFactory.id(literal, ser);
			current = org.apache.commons.lang3.tuple.Triple.of(id, ser.rewind(), rdfFactory);
			cachedIV = current;
		} else if (current.getMiddle() == null) {
			ser = rdfFactory.getSerializedForm(literal);
			current = org.apache.commons.lang3.tuple.Triple.of(current.getLeft(), ser, rdfFactory);
			cachedIV = current;
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefaultWriter().toBytes(literal);
		return new SerializedValue(b);
	}
}
