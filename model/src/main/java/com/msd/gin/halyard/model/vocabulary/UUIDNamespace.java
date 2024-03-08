package com.msd.gin.halyard.model.vocabulary;

import com.msd.gin.halyard.common.ByteUtils;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class UUIDNamespace extends AbstractIRIEncodingNamespace {
	private static final long serialVersionUID = 4317476337424799955L;

	UUIDNamespace(String prefix, String ns) {
		super(prefix, ns);
	}

	@Override
	public ByteBuffer writeBytes(String localName, ByteBuffer b) {
		UUID uuid = UUID.fromString(localName);
		b = ByteUtils.ensureCapacity(b, Long.BYTES + Long.BYTES);
		b.putLong(uuid.getMostSignificantBits());
		b.putLong(uuid.getLeastSignificantBits());
		return b;
	}

	@Override
	public String readBytes(ByteBuffer b) {
		long uuidMost = b.getLong();
		long uuidLeast = b.getLong();
		UUID uuid = new UUID(uuidMost, uuidLeast);
		return uuid.toString();
	}
}
