package com.msd.gin.halyard.model.vocabulary;

import java.nio.ByteBuffer;

import com.msd.gin.halyard.common.ByteUtils;

public class Base64Namespace extends AbstractIRIEncodingNamespace {
	private static final long serialVersionUID = 2008223953375980683L;

	Base64Namespace(String prefix, String ns) {
		super(prefix, ns);
	}

	@Override
	public ByteBuffer writeBytes(String localName, ByteBuffer b) {
		byte[] b64 = ByteUtils.decode(localName);
		b = ByteUtils.ensureCapacity(b, b64.length);
		b.put(b64);
		return b;
	}

	@Override
	public String readBytes(ByteBuffer b) {
		return ByteUtils.encode(b).toString();
	}
}
