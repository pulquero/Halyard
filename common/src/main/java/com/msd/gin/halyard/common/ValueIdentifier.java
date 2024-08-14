package com.msd.gin.halyard.common;

import com.msd.gin.halyard.common.Hashes.HashFunction;
import com.msd.gin.halyard.model.LiteralConstraint;
import com.msd.gin.halyard.model.ValueType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Immutable wrapper around a byte array identifier.
 */
public final class ValueIdentifier extends ByteSequence implements Serializable {
	private static final long serialVersionUID = 1293499350691875714L;

	enum TypeNibble {
		// leading 4 bits
		BIG_NIBBLE(
			(byte) 0x00 /* literal type bits */,
			(byte) 0x40 /* triple type bits */,
			(byte) 0x80 /* IRI type bits */,
			(byte) 0xC0 /* BNode type bits */,
			(byte) 0x3F /* size of each type */,

			(byte) 0x00 /* non-string datatype bits */,
			(byte) 0x20 /* string datatype bits */
		),
		// trailing 4 bits
		LITTLE_NIBBLE(
			(byte) 0x00 /* literal type bits */,
			(byte) 0x04 /* triple type bits */,
			(byte) 0x08 /* IRI type bits */,
			(byte) 0x0C /* BNode type bits */,
			(byte) 0x03 /* size of each type */,

			(byte) 0x00 /* non-string datatype bits */,
			(byte) 0x02 /* string datatype bits */
		);

		final byte literalTypeBits;
		final byte tripleTypeBits;
		final byte iriTypeBits;
		final byte bnodeTypeBits;
		final byte typeSize;
		final byte typeMask;
		final byte clearTypeMask;
		final byte nonstringDatatypeBits;
		final byte stringDatatypeBits;
		final byte datatypeMask;
		final byte clearDatatypeMask;

		private TypeNibble(byte literalTypeBits, byte tripleTypeBits, byte iriTypeBits, byte bnodeTypeBits, byte typeSize, byte nonstringDatatypeBits, byte stringDatatypeBits) {
			this.literalTypeBits = literalTypeBits;
			this.tripleTypeBits = tripleTypeBits;
			this.iriTypeBits = iriTypeBits;
			this.bnodeTypeBits = bnodeTypeBits;
			this.typeSize = typeSize;
			this.typeMask = bnodeTypeBits;
			this.clearTypeMask = (byte) ~typeMask;
			this.nonstringDatatypeBits = nonstringDatatypeBits;
			this.stringDatatypeBits = stringDatatypeBits;
			this.datatypeMask = stringDatatypeBits;
			this.clearDatatypeMask = (byte) ~datatypeMask;
		}
	}

	@ThreadSafe
	static final class Format implements Serializable {
		private static final long serialVersionUID = -7777885367792871664L;

		static int minSize(int typeIndex, boolean hasJavaHash) {
			// salt & typing bytes + 4 bytes for the Java int hash code
			return typeIndex + 1 + (hasJavaHash ? Integer.BYTES : 0);
		}

		final ValueFactory valueFactory = SimpleValueFactory.getInstance();
		final String algorithm;
		final int size;
		final int typeIndex;
		final TypeNibble typeNibble;
		final boolean hasJavaHash;
		transient ThreadLocal<HashFunction> hashFuncProvider;

		/**
		 * Identifier format.
		 * @param algorithm algorithm used to generate the ID.
		 * @param size byte size of the ID
		 * @param typeIndex byte index to store type information
		 * @param typeNibble byte nibble to store type information
		 * @param hasJavaHash indicates whether to include the Java hash as part of the ID.
		 */
		Format(String algorithm, int size, int typeIndex, TypeNibble typeNibble, boolean hasJavaHash) {
			if (hasJavaHash && size < minSize(typeIndex, hasJavaHash)) {
				throw new IllegalArgumentException("Size is too small");
			}
			this.size = size;
			this.algorithm = algorithm;
			this.typeIndex = typeIndex;
			this.typeNibble = typeNibble;
			this.hasJavaHash = hasJavaHash;
			initHashProvider();
		}

		private void initHashProvider() {
			int algoSize = hasJavaHash ? size - Integer.BYTES : size;
			if (algoSize > 0) {
				hashFuncProvider = new ThreadLocal<HashFunction>() {
					@Override
					protected HashFunction initialValue() {
						return Hashes.getHash(algorithm, algoSize);
					}
				};
			}
		}

		/**
		 * Returns the number of possible salt values.
		 */
		int getSaltSize() {
			int typeSaltSize;
			switch (typeNibble) {
				case BIG_NIBBLE:
					typeSaltSize = 1 << (8*typeIndex);
					break;
				case LITTLE_NIBBLE:
					typeSaltSize = 1 << (8*typeIndex+4);
					break;
				default:
					throw new AssertionError();
			}
			return typeSaltSize;
		}

		/**
		 * Thread-safe.
		 */
		ValueIdentifier id(ByteSequence ser, ValueIO.Reader reader) {
			byte[] hash = new byte[size];
			ByteBuffer serbb = ser.asReadOnlyBuffer();
			if (hashFuncProvider != null) {
				byte[] algoHash = hashFuncProvider.get().apply(serbb);
				System.arraycopy(algoHash, 0, hash, 0, algoHash.length);
			}
			serbb.rewind();
			ValueType type = reader.getValueType(serbb);
			CoreDatatype datatype;
			Value val = null;
			if (type == ValueType.LITERAL) {
				datatype = reader.getCoreDatatype(serbb);
				if (datatype == null) {
					val = reader.readValue(serbb, valueFactory);
					datatype = ((Literal)val).getCoreDatatype();
				}
			} else {
				datatype = null;
			}

			if (hasJavaHash) {
				if (val == null) {
					val = reader.readValue(serbb, valueFactory);
				}
				int jhash = val.hashCode();
				int i = size - 1;
				hash[i--] = (byte) jhash;
				jhash >>>= 8;
				hash[i--] = (byte) jhash;
				jhash >>>= 8;
				hash[i--] = (byte) jhash;
				jhash >>>= 8;
				hash[i--] = (byte) jhash;
			}
			writeType(type, datatype, hash, 0);
			return new ValueIdentifier(hash);
		}

		@Override
		public String toString() {
			String javaHash = hasJavaHash ? "+JavaHash" : "";
			return String.format("%s%s %d-bit (%d bytes), layout: %d %s (salts: %d)", algorithm, javaHash, size*Byte.SIZE, size, typeIndex, typeNibble, getSaltSize());
		}

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (!(other instanceof Format)) {
				return false;
			}
			Format that = (Format) other;
			return algorithm.equals(that.algorithm)
				&& size == that.size
				&& typeIndex == that.typeIndex
				&& typeNibble == that.typeNibble
				&& hasJavaHash == that.hasJavaHash;
		}

		@Override
		public int hashCode() {
			return Objects.hash(algorithm, size, typeIndex, typeNibble, hasJavaHash);
		}

		private int getTypeBits(byte[] idBytes) {
			return (idBytes[typeIndex] & typeNibble.typeMask);
		}

		private int getDatatypeBits(byte[] idBytes) {
			return (idBytes[typeIndex] & typeNibble.datatypeMask);
		}

		byte[] unrotate(byte[] src, int offset, int len, int shift, byte[] dest) {
			byte[] rotated = rotateLeft(src, offset, len, shift, dest);
			if (shift != 0) {
				// preserve position of type byte
				int shiftedTypeIndex = (typeIndex + len - shift) % len;
				byte typeByte = rotated[shiftedTypeIndex];
				byte tmp = rotated[typeIndex];
				rotated[typeIndex] = typeByte;
				rotated[shiftedTypeIndex] = tmp;
			}
			return rotated;
		}

		byte[] writeType(ValueType type, CoreDatatype datatype, byte[] arr, int offset) {
			int typeBits;
			int dtBits = 0;
			switch (type) {
				case LITERAL:
					typeBits = typeNibble.literalTypeBits;
					dtBits = LiteralConstraint.isString(datatype) ? typeNibble.stringDatatypeBits : typeNibble.nonstringDatatypeBits;
					break;
				case TRIPLE:
					typeBits = typeNibble.tripleTypeBits;
					break;
				case IRI:
					typeBits = typeNibble.iriTypeBits;
					break;
				case BNODE:
					typeBits = typeNibble.bnodeTypeBits;
					break;
				default:
					throw new AssertionError();
			}
			byte typeByte = (byte) ((arr[offset+typeIndex] & typeNibble.clearTypeMask) | typeBits);
			if (datatype != null) {
				typeByte = (byte) ((typeByte & typeNibble.clearDatatypeMask) | dtBits);
			}
			arr[offset+typeIndex] = typeByte;
			return arr;
		}

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			in.defaultReadObject();
			initHashProvider();
		}

		static byte[] rotateLeft(byte[] src, int offset, int len, int shift, byte[] dest) {
			if(shift > len) {
				shift = shift % len;
			}
			if (shift != 0) {
				System.arraycopy(src, offset+shift, dest, 0, len-shift);
				System.arraycopy(src, offset, dest, len-shift, shift);
			} else {
				System.arraycopy(src, offset, dest, 0, len);
			}
			return dest;
		}

		static byte[] rotateRight(byte[] src, int offset, int len, int shift, byte[] dest) {
			if(shift > len) {
				shift = shift % len;
			}
			if (shift != 0) {
				System.arraycopy(src, offset+len-shift, dest, 0, shift);
				System.arraycopy(src, offset, dest, shift, len-shift);
			} else {
				System.arraycopy(src, offset, dest, 0, len);
			}
			return dest;
		}
	}

	private final byte[] idBytes;

	ValueIdentifier(byte[] idBytes) {
		this.idBytes = idBytes;
	}

	@Override
	public int size() {
		return idBytes.length;
	}

	public boolean isIRI(Format format) {
		return format.getTypeBits(idBytes) == format.typeNibble.iriTypeBits;
	}

	public boolean isLiteral(Format format) {
		return format.getTypeBits(idBytes) == format.typeNibble.literalTypeBits;
	}

	public boolean isBNode(Format format) {
		return format.getTypeBits(idBytes) == format.typeNibble.bnodeTypeBits;
	}

	public boolean isTriple(Format format) {
		return format.getTypeBits(idBytes) == format.typeNibble.tripleTypeBits;
	}

	public boolean isString(Format format) {
		return isLiteral(format) && (format.getDatatypeBits(idBytes) == format.typeNibble.stringDatatypeBits);
	}

	@Override
	public ByteBuffer writeTo(ByteBuffer bb) {
		return bb.put(idBytes);
	}

	@Override
	public ByteBuffer asReadOnlyBuffer() {
		return ByteBuffer.wrap(idBytes).asReadOnlyBuffer();
	}

	ByteBuffer writeSliceTo(int offset, int len, ByteBuffer bb) {
		return bb.put(idBytes, offset, len);
	}

	byte[] rotate(int len, int shift, byte[] dest, Format format) {
		byte[] rotated = Format.rotateRight(idBytes, 0, len, shift, dest);
		if (shift != 0) {
			// preserve position of type byte
			int shiftedTypeIndex = (format.typeIndex + shift) % len;
			byte typeByte = rotated[shiftedTypeIndex];
			byte tmp = rotated[format.typeIndex];
			rotated[format.typeIndex] = typeByte;
			rotated[shiftedTypeIndex] = tmp;
		}
		return rotated;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ValueIdentifier)) {
			return false;
		}
		ValueIdentifier that = (ValueIdentifier) o;
		return Arrays.equals(this.idBytes, that.idBytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(idBytes);
	}

	int valueHashCode(Format format) {
		if (!format.hasJavaHash) {
			throw new IllegalArgumentException("Java hash not available");
		}
		int i = idBytes.length - Integer.BYTES;
		return (idBytes[i++] & 0xFF) << 24 | (idBytes[i++] & 0xFF) << 16 | (idBytes[i++] & 0xFF) << 8 | (idBytes[i++] & 0xFF);
	}

	@Override
	public String toString() {
		return ByteUtils.encode(idBytes);
	}
}
