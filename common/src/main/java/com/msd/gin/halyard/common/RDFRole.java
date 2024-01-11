package com.msd.gin.halyard.common;

import com.msd.gin.halyard.common.ValueIdentifier.Format;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

@ThreadSafe
public final class RDFRole<T extends SPOC<?>> {
	public enum Name {
		SUBJECT {
			@Override
			public <E> E getValue(E s, E p, E o, E c) {
				return s;
			}
			@Override
			public Value getValue(Statement s) {
				return s.getSubject();
			}
		},
		PREDICATE {
			@Override
			public <E> E getValue(E s, E p, E o, E c) {
				return p;
			}
			@Override
			public Value getValue(Statement s) {
				return s.getPredicate();
			}
		},
		OBJECT {
			@Override
			public <E> E getValue(E s, E p, E o, E c) {
				return o;
			}
			@Override
			public Value getValue(Statement s) {
				return s.getObject();
			}
		},
		CONTEXT {
			@Override
			public <E> E getValue(E s, E p, E o, E c) {
				return c;
			}
			@Override
			public Value getValue(Statement s) {
				return s.getContext();
			}
		};

		public abstract <E> E getValue(E s, E p, E o, E c);
		public abstract Value getValue(Statement s);
	}
	private final Name name;
	private final int idSize;
	private final int keyHashSize;
	private final ByteSequence startKey;
	private final ByteSequence stopKey;
	private final int shift;
	private final int sizeLength;

	public RDFRole(Name name, int idSize, int keyHashSize, int shift, int sizeLength, boolean required) {
		this.name = name;
		this.idSize = idSize;
		this.keyHashSize = keyHashSize;
		this.startKey = required ? new ByteFiller((byte)0x00, keyHashSize) : ByteSequence.EMPTY;
		this.stopKey = new ByteFiller((byte)0xFF, keyHashSize);
		this.shift = shift;
		this.sizeLength = sizeLength;
	}

	Name getName() {
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
