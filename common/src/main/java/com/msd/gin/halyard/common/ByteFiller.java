package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class ByteFiller extends ByteSequence {
	private final byte value;
	private final int size;

	public ByteFiller(byte value, int size) {
		this.value = value;
		this.size = size;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public ByteBuffer writeTo(ByteBuffer bb) {
		if (bb.hasArray()) {
			int pos = bb.position();
			int startIndex = bb.arrayOffset() + pos;
			Arrays.fill(bb.array(), startIndex, startIndex + size, value);
			bb.position(pos+size);
		} else {
			for (int i=0; i<size; i++) {
				bb.put(value);
			}
		}
		return bb;
	}

	@Override
	public byte[] copyBytes() {
		byte[] copy = new byte[size];
		Arrays.fill(copy, value);
		return copy;
	}
}
