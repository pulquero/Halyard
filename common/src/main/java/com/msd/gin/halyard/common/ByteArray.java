package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Immutable byte array.
 */
public final class ByteArray extends ByteSequence {
	private final byte[] arr;

	public ByteArray(byte[] arr) {
		this.arr = arr;
	}

	public int get(int index) {
		return arr[index];
	}

	@Override
	public int size() {
		return arr.length;
	}

	@Override
	public ByteBuffer writeTo(ByteBuffer bb) {
		return bb.put(arr);
	}

	public byte[] copyBytes() {
		byte[] copy = new byte[arr.length];
		System.arraycopy(arr, 0, copy, 0, arr.length);
		return copy;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof ByteArray) {
			ByteArray that = (ByteArray) o;
			return Arrays.equals(this.arr, that.arr);
		} else {
			return super.equals(o);
		}
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(arr);
	}
}
