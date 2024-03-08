package com.msd.gin.halyard.common;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;

public final class ByteUtils {
	private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
	private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

	private ByteUtils() {}

	public static ByteBuffer ensureCapacity(ByteBuffer b, int requiredSize) {
		if (b.remaining() < requiredSize) {
			// leave some spare capacity
			ByteBuffer newb = ByteBuffer.allocate(3*b.capacity()/2 + 2*requiredSize);
			b.flip();
			newb.put(b);
			return newb;
		} else {
			return b;
		}
	}

	/**
	 * Encode a byte array to a base-64 string.
	 * @param b array to encode.
	 * @return base-64 string.
	 */
	public static String encode(byte b[]) {
	    return ENCODER.encodeToString(b);
	}

	/**
	 * NB: this alters the buffer.
	 */
	public static CharSequence encode(ByteBuffer b) {
		return StandardCharsets.ISO_8859_1.decode(ENCODER.encode(b));
	}

	/**
	 * Decode a base-64 string to a byte array.
	 * @param s string to decode.
	 * @return byte array.
	 */
	public static byte[] decode(String s) {
		return DECODER.decode(s);
	}


	public static byte[] fromString(String s) {
		return s.getBytes(StandardCharsets.UTF_8);
	}

	public static String toString(byte[] b) {
		return new String(b, 0, b.length, StandardCharsets.UTF_8);
	}

	public static byte[] fromHexString(String s) {
		try {
			return Hex.decodeHex(s.toCharArray());
		} catch (DecoderException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static String toHexString(byte[] b) {
		return new String(Hex.encodeHex(b));
	}

	
	static final int MAX_LONG_STRING_LENGTH = Long.toString(Long.MAX_VALUE).length();

	public static ByteBuffer writeCompressedInteger(String s, ByteBuffer b) {
		BigInteger bigInt;
		long x;
		if (s.length() < MAX_LONG_STRING_LENGTH) {
			// definitely long or smaller
			x = XMLDatatypeUtil.parseLong(s);
			bigInt = null;
		} else {
			bigInt = XMLDatatypeUtil.parseInteger(s);
			x = 0;
		}
		return writeCompressedInteger(bigInt, x, b);
	}

	public static ByteBuffer writeCompressedInteger(BigInteger bigInt, long x, ByteBuffer b) {
		if (bigInt != null) {
			double u = bigInt.doubleValue();
			if (u >= Long.MIN_VALUE && u <= Long.MAX_VALUE) {
				x = bigInt.longValueExact();
				bigInt = null;
			}
		}
		if (bigInt != null) {
			byte[] bytes = bigInt.toByteArray();
			b = ByteUtils.ensureCapacity(b, 1 + bytes.length);
			return b.put(HeaderBytes.BIG_INT_TYPE).put(bytes);
		} else if (x >= Short.MIN_VALUE && x <= Short.MAX_VALUE) {
			b = ByteUtils.ensureCapacity(b, 1 + Short.BYTES);
			return b.put(HeaderBytes.SHORT_COMPRESSED_BIG_INT_TYPE).putShort((short) x);
		} else if (x >= Integer.MIN_VALUE && x <= Integer.MAX_VALUE) {
			b = ByteUtils.ensureCapacity(b, 1 + Integer.BYTES);
			return b.put(HeaderBytes.INT_COMPRESSED_BIG_INT_TYPE).putInt((int) x);
		} else {
			b = ByteUtils.ensureCapacity(b, 1 + Long.BYTES);
			return b.put(HeaderBytes.LONG_COMPRESSED_BIG_INT_TYPE).putLong(x);
		}
	}

	public static String readCompressedInteger(ByteBuffer b) {
		int type = b.get();
		switch (type) {
			case HeaderBytes.SHORT_COMPRESSED_BIG_INT_TYPE:
				return Short.toString(b.getShort());
			case HeaderBytes.INT_COMPRESSED_BIG_INT_TYPE:
				return Integer.toString(b.getInt());
			case HeaderBytes.LONG_COMPRESSED_BIG_INT_TYPE:
				return Long.toString(b.getLong());
			case HeaderBytes.BIG_INT_TYPE:
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return new BigInteger(bytes).toString();
			default:
				throw new AssertionError(String.format("Unrecognized compressed integer type: %d", type));
		}
	}
}
