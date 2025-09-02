package com.msd.gin.halyard.model.vocabulary;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.msd.gin.halyard.common.ByteUtils;

public class OrcidNamespace extends AbstractIRIEncodingNamespace {
	private static final long serialVersionUID = -8193568694174435059L;

	public OrcidNamespace(String prefix, String name) {
		super(prefix, name);
	}

	@Override
	public ByteBuffer writeBytes(String localName, ByteBuffer b) {
		String numWithChecksum;
		if (localName.length() == 19) {
			numWithChecksum = localName.replace("-", "");
		} else {
			numWithChecksum = localName;
		}
		if (numWithChecksum.length() != 16) {
			throw new IllegalArgumentException(String.format("Invalid length for ORCID: %s", localName));
		}
		int checksumPos = numWithChecksum.length()-1;
		char checksum = numWithChecksum.charAt(checksumPos);
		// prefix with 1 to maintain leading zeros
		BigInteger id = new BigInteger("1"+numWithChecksum.substring(0, checksumPos));
		byte[] bytes = id.toByteArray();
		b = ByteUtils.ensureCapacity(b, 1+bytes.length+1);
		return b.put((byte) bytes.length).put(bytes).put((byte)checksum);
	}

	@Override
	public String readBytes(ByteBuffer b) {
		int len = b.get();
		byte[] idBytes = new byte[len];
		b.get(idBytes);
		char checksum = (char) b.get();
		BigInteger id = new BigInteger(idBytes);
		String idStr = id.toString();
		return idStr.substring(1, 5) + "-" + idStr.substring(5, 9) + "-" + idStr.substring(9, 13) + "-" + idStr.substring(13) + checksum;
	}
}
