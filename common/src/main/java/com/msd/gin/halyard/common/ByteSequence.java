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


	public static ByteSequence concat(ByteSequence b1, ByteSequence b2) {
		return new ByteSequence() {
			@Override
			public ByteBuffer writeTo(ByteBuffer bb) {
				b1.writeTo(bb);
				b2.writeTo(bb);
				return bb;
			}

			@Override
			public int size() {
				return b1.size() + b2.size();
			}
		};
	}

	public static ByteSequence concat(ByteSequence b1, ByteSequence b2, ByteSequence b3) {
		return new ByteSequence() {
			@Override
			public ByteBuffer writeTo(ByteBuffer bb) {
				b1.writeTo(bb);
				b2.writeTo(bb);
				b3.writeTo(bb);
				return bb;
			}

			@Override
			public int size() {
				return b1.size() + b2.size() + b3.size();
			}
		};
	}
}
