package com.msd.gin.halyard.model.vocabulary;

import java.nio.ByteBuffer;

import com.msd.gin.halyard.common.ByteUtils;

public class IntegerNamespace extends AbstractIRIEncodingNamespace {
	private static final long serialVersionUID = 571723932297421854L;

	IntegerNamespace(String prefix, String ns) {
		super(prefix, ns);
	}

	@Override
	public ByteBuffer writeBytes(String localName, ByteBuffer b) {
		return ByteUtils.writeCompressedInteger(localName, b);
	}

	@Override
	public String readBytes(ByteBuffer b) {
		return ByteUtils.readCompressedInteger(b);
	}
}
