package com.msd.gin.halyard.common;

import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.rdf4j.model.Value;

public abstract class RDFValue<V extends Value> {
	final V val;
	private byte[] ser;
	private byte[] hash;

	public static <V extends Value> boolean matches(V value, RDFValue<V> pattern) {
		return pattern == null || pattern.val.equals(value);
	}

	protected RDFValue(V val) {
		this.val = val;
	}

	public final byte[] getSerializedForm() {
		if (ser == null) {
			ser = HalyardTableUtils.writeBytes(val);
		}
		return ser;
	}

	private final byte[] getUniqueHash() {
		if (hash == null) {
			if (val instanceof Identifiable) {
				Identifiable idVal = (Identifiable) val;
				hash = idVal.getId();
				if (hash == null) {
					hash = HalyardTableUtils.id(val);
					idVal.setId(hash);
				}
			} else {
				hash = HalyardTableUtils.id(val);
			}
		}
		return hash;
	}

	public final byte[] getKeyHash() {
		return Bytes.copy(getUniqueHash(), 0, keyHashSize());
	}

	final byte[] getEndKeyHash() {
		return Bytes.copy(getUniqueHash(), 0, endKeyHashSize());
	}

	final byte[] getQualifierHash() {
		return Bytes.copy(getUniqueHash(), keyHashSize(), qualifierHashSize());
	}

	final byte[] getEndQualifierHash() {
		return Bytes.copy(getUniqueHash(), endKeyHashSize(), endQualifierHashSize());
	}

	protected abstract int keyHashSize();

	protected abstract int endKeyHashSize();

	final int qualifierHashSize() {
		return getUniqueHash().length - keyHashSize();
	}

	final int endQualifierHashSize() {
		return getUniqueHash().length - endKeyHashSize();
	}
}
