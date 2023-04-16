package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.BNode;

public final class IdentifiableBNode extends BNodeWrapper implements IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = -6212507967580561560L;
	private transient org.apache.commons.lang3.tuple.Triple<ValueIdentifier,ByteBuffer,RDFFactory> cachedIV = org.apache.commons.lang3.tuple.Triple.of(null, null, null);

	IdentifiableBNode(BNode bnode) {
		super(bnode);
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		org.apache.commons.lang3.tuple.Triple<ValueIdentifier,ByteBuffer,RDFFactory> current = cachedIV;
		ValueIdentifier id = current.getLeft();
		if (current.getRight() != rdfFactory) {
			ByteBuffer ser = rdfFactory.getSerializedForm(bnode);
			id = rdfFactory.id(bnode, ser);
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
			ser = rdfFactory.getSerializedForm(bnode);
			ValueIdentifier id = rdfFactory.id(bnode, ser);
			current = org.apache.commons.lang3.tuple.Triple.of(id, ser.rewind(), rdfFactory);
			cachedIV = current;
		} else if (current.getMiddle() == null) {
			ser = rdfFactory.getSerializedForm(bnode);
			current = org.apache.commons.lang3.tuple.Triple.of(current.getLeft(), ser, rdfFactory);
			cachedIV = current;
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefaultWriter().toBytes(bnode);
		return new SerializedValue(b);
	}
}
