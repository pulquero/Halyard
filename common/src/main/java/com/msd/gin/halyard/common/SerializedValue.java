package com.msd.gin.halyard.common;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

final class SerializedValue implements Externalizable {
	private byte[] ser;

	public SerializedValue() {}

	public SerializedValue(byte[] ser) {
		this.ser = ser;
	}

	private Object readResolve() throws ObjectStreamException {
		return ValueIO.getDefaultReader().readValue(ByteBuffer.wrap(ser), new IdValueFactory(null));
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(ser.length);
		out.write(ser);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		int size = in.readInt();
		ser = new byte[size];
		in.readFully(ser);
	}
}
