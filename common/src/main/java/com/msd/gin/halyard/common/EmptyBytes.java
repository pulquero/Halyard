package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;

final class EmptyBytes extends ByteSequence {
	private static final byte[] EMPTY_BYTES = new byte[0];

	@Override
	public ByteBuffer writeTo(ByteBuffer bb) {
		return bb;
	}

	@Override
	public byte[] copyBytes() {
		return EMPTY_BYTES;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof ByteSequence) {
			ByteSequence that = (ByteSequence) o;
			return that.size() == 0;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return 0;
	}
}
