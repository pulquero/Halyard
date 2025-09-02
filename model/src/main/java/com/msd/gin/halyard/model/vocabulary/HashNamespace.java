package com.msd.gin.halyard.model.vocabulary;

import java.nio.ByteBuffer;

import com.msd.gin.halyard.common.ByteUtils;

public class HashNamespace extends AbstractIRIEncodingNamespace {
	private static final long serialVersionUID = 2218386262774783440L;

	HashNamespace(String prefix, String ns) {
		super(prefix, ns);
	}

	@Override
	public ByteBuffer writeBytes(String localName, ByteBuffer b) {
		byte[] hexBytes = ByteUtils.fromHexString(localName);
		b = ByteUtils.ensureCapacity(b, hexBytes.length);
		b.put(hexBytes);
		return b;
	}

	@Override
	public String readBytes(ByteBuffer b) {
		byte[] hexBytes = new byte[b.remaining()];
		b.get(hexBytes);
		return ByteUtils.toHexString(hexBytes);
	}
}
