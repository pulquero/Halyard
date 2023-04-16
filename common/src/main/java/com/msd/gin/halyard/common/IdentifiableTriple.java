package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.Triple;

public final class IdentifiableTriple extends TripleWrapper implements IdentifiableValue, SerializableValue {
	private static final long serialVersionUID = 228285959274911416L;
	private transient org.apache.commons.lang3.tuple.Triple<ValueIdentifier,ByteBuffer,RDFFactory> cachedIV = org.apache.commons.lang3.tuple.Triple.of(null, null, null);

	IdentifiableTriple(Triple triple) {
		super(triple);
	}

	@Override
	public ValueIdentifier getId(RDFFactory rdfFactory) {
		org.apache.commons.lang3.tuple.Triple<ValueIdentifier,ByteBuffer,RDFFactory> current = cachedIV;
		ValueIdentifier id = current.getLeft();
		if (current.getRight() != rdfFactory) {
			ByteBuffer ser = rdfFactory.getSerializedForm(triple);
			id = rdfFactory.id(triple, ser);
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
			ser = rdfFactory.getSerializedForm(triple);
			ValueIdentifier id = rdfFactory.id(triple, ser);
			current = org.apache.commons.lang3.tuple.Triple.of(id, ser.rewind(), rdfFactory);
			cachedIV = current;
		} else if (current.getMiddle() == null) {
			ser = rdfFactory.getSerializedForm(triple);
			current = org.apache.commons.lang3.tuple.Triple.of(current.getLeft(), ser, rdfFactory);
			cachedIV = current;
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		byte[] b = ValueIO.getDefaultWriter().toBytes(triple);
		return new SerializedValue(b);
	}
}
