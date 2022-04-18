package com.msd.gin.halyard.common;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.eclipse.rdf4j.model.Literal;

public final class IdentifiableLiteral extends LiteralWrapper implements Identifiable, SerializableValue {
	private static final long serialVersionUID = 4299930477670062440L;
	private final RDFFactory rdfFactory;
	private Identifier id;
	private ByteBuffer ser;

	IdentifiableLiteral(Literal literal, RDFFactory valueIO) {
		super(literal);
		this.rdfFactory = Objects.requireNonNull(valueIO);
	}

	@Override
	public Identifier getId() {
		if (id == null) {
			id = rdfFactory.id(literal, getSerializedForm());
		}
		return id;
	}

	@Override
	public void setId(Identifier id) {
		this.id = id;
	}

	@Override
	public ByteBuffer getSerializedForm() {
		if (ser == null) {
			byte[] b = rdfFactory.ID_TRIPLE_WRITER.toBytes(literal);
			ser = ByteBuffer.wrap(b).asReadOnlyBuffer();
		}
		return ser.duplicate();
	}

	private Object writeReplace() throws ObjectStreamException {
		ByteBuffer serBuf = getSerializedForm();
		byte[] b = new byte[serBuf.remaining()];
		serBuf.get(b);
		return new SerializedValue(b, rdfFactory.STREAM_READER);
	}
}
