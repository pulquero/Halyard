package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Immutable sequence of bytes.
 */
public abstract class ByteSequence {
	public static final ByteSequence EMPTY = new EmptyBytes();

	public abstract ByteBuffer writeTo(ByteBuffer bb);
	public abstract int size();

	public byte[] copyBytes() {
		byte[] copy = new byte[size()];
		writeTo(ByteBuffer.wrap(copy));
		return copy;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof ByteSequence) {
			ByteSequence that = (ByteSequence) o;
			return Arrays.equals(this.copyBytes(), that.copyBytes());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(copyBytes());
	}

	@Override
	public String toString() {
		return Arrays.toString(copyBytes());
	}
}
