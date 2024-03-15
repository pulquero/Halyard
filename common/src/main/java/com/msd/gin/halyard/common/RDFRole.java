package com.msd.gin.halyard.common;

import com.msd.gin.halyard.common.ValueIdentifier.Format;
import com.msd.gin.halyard.model.TermRole;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class RDFRole<T extends SPOC<?>> {
	private final TermRole name;
	private final int idSize;
	private final int keyHashSize;
	private final ByteSequence startKey;
	private final ByteSequence stopKey;
	private final int shift;
	private final int sizeLength;

	public RDFRole(TermRole name, int idSize, int keyHashSize, int shift, int sizeLength, boolean required) {
		this.name = name;
		this.idSize = idSize;
		this.keyHashSize = keyHashSize;
		this.startKey = required ? new ByteFiller((byte)0x00, keyHashSize) : ByteSequence.EMPTY;
		this.stopKey = new ByteFiller((byte)0xFF, keyHashSize);
		this.shift = shift;
		this.sizeLength = sizeLength;
	}

	TermRole getName() {
		return name;
	}

	RDFValue<?,T> getValue(RDFValue<?,?> s, RDFValue<?,?> p, RDFValue<?,?> o, RDFValue<?,?> c) {
		return (RDFValue<?,T>) name.getValue(s, p, o, c);
	}

	/**
	 * Key hash size in bytes.
	 * @return size in bytes.
	 */
	public int keyHashSize() {
		return keyHashSize;
	}

	int qualifierHashSize() {
		return idSize - keyHashSize;
	}

	int sizeLength() {
		return sizeLength;
	}

	byte[] keyHash(ValueIdentifier id, Format format) {
		// rotate key so ordering is different for different prefixes
		// this gives better load distribution when traversing between prefixes
		return keyHashSize > 0 ? id.rotate(keyHashSize, shift, new byte[keyHashSize], format) : new byte[0];
	}

	ByteBuffer writeQualifierHashTo(ValueIdentifier id, ByteBuffer bb) {
		return id.writeSliceTo(keyHashSize(), qualifierHashSize(), bb);
	}

	ByteSequence startKey() {
		return startKey;
	}

	ByteSequence stopKey() {
		return stopKey;
	}

	int getByteShift() {
		return shift;
	}

	@Override
	public String toString() {
		return name.toString();
	}
}
