package com.msd.gin.halyard.model.vocabulary;

import java.nio.ByteBuffer;

import com.msd.gin.halyard.common.ByteUtils;

public class IntegerPairNamespace extends AbstractIRIEncodingNamespace {
	private static final long serialVersionUID = 571723932297421854L;

	private final String sep1;
	private final String sep2;

	IntegerPairNamespace(String prefix, String ns, String sep1, String sep2) {
		super(prefix, ns);
		this.sep1 = sep1;
		this.sep2 = sep2;
	}

	@Override
	public ByteBuffer writeBytes(String localName, ByteBuffer b) {
		int start1 = sep1.length();
		int end1 = localName.indexOf(sep2, start1);
		if (end1 == -1) {
			throw new IllegalArgumentException("Unexpected input: "+localName);
		}
		int start2 =  end1 + sep2.length();
		int start = b.position();
		b = ByteUtils.ensureCapacity(b, 1);
		b.position(start + 1);
		b = ByteUtils.writeCompressedInteger(localName.substring(start1, end1), b);
		int len1 = b.position() - start - 1;
		b = ByteUtils.writeCompressedInteger(localName.substring(start2), b);
		b.put(start, (byte)len1);
		return b;
	}

	@Override
	public String readBytes(ByteBuffer b) {
		int len1 = b.get();
		int originalLimit = b.limit();
		b.limit(b.position() + len1);
		String first = ByteUtils.readCompressedInteger(b);
		b.limit(originalLimit);
		String second = ByteUtils.readCompressedInteger(b);
		return sep1 + first + sep2 + second;
	}
}
