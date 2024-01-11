package com.msd.gin.halyard.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComponentComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

/**
 * Triples/quads are stored in multiple indices as different permutations.
 */
@ThreadSafe
public final class StatementIndex<T1 extends SPOC<?>,T2 extends SPOC<?>,T3 extends SPOC<?>,T4 extends SPOC<?>> {
	public enum Name {
		SPO(false), POS(false), OSP(false), CSPO(true), CPOS(true), COSP(true);

		private final boolean isQuad;
		Name(boolean isQuad) {
			this.isQuad = isQuad;
		}
		public boolean isQuadIndex() {
			return this.isQuad;
		}
	};
	private static final byte WELL_KNOWN_IRI_MARKER = (byte) ('#' | 0x80);  // marker must be negative (msb set) so it is distinguishable from a length (>=0)

	private static final int SUBJECT_VAR_CARDINALITY = 1000;
	private static final int PREDICATE_VAR_CARDINALITY = 10;
	private static final int OBJECT_VAR_CARDINALITY = 2000;  // usually more objects than subjects
	private static final int CONTEXT_VAR_CARDINALITY = 3;

	private static int getRoleCardinality(RDFRole.Name role) {
		switch (role) {
			case SUBJECT:
				return SUBJECT_VAR_CARDINALITY;
			case PREDICATE:
				return PREDICATE_VAR_CARDINALITY;
			case OBJECT:
				return OBJECT_VAR_CARDINALITY;
			case CONTEXT:
				return CONTEXT_VAR_CARDINALITY;
			default:
				throw new AssertionError();
		}
	}

	/**
	 * @param sizeLen length of size field, 2 for short, 4 for int.
	 */
	private static int valueSize(RDFValue<?,?> v, int sizeLen) {
		if (v.isWellKnownIRI()) {
			return 1;
		} else {
			return sizeLen + v.getSerializedForm().size();
		}
	}

	private static void putRDFValue(ByteBuffer cv, RDFValue<?,?> v, int sizeLen) {
		if (v.isWellKnownIRI()) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			ByteSequence ser = v.getSerializedForm();
			switch (sizeLen) {
				case Short.BYTES:
					cv.putShort((short) ser.size());
					ser.writeTo(cv);
					break;
				case Integer.BYTES:
					cv.putInt(ser.size());
					ser.writeTo(cv);
					break;
				default:
					throw new AssertionError("Unsupported sizeLen: "+sizeLen);
			}
		}
	}

	private static void putLastRDFValue(ByteBuffer cv, RDFValue<?,?> v) {
		if (v.isWellKnownIRI()) {
			cv.put(WELL_KNOWN_IRI_MARKER);
		} else {
			ByteSequence ser = v.getSerializedForm();
			ser.writeTo(cv);
		}
	}

	private static void skipId(ByteBuffer key, ByteBuffer cn, int keySize, int idSize) {
		key.position(key.position() + keySize);
		cn.position(cn.position() + idSize - keySize);
	}

	private static ByteSequence concat2(ByteSequence b1, ByteSequence b2) {
		return new ByteSequence() {
			@Override
			public ByteBuffer writeTo(ByteBuffer bb) {
				b1.writeTo(bb);
				b2.writeTo(bb);
				return bb;
			}

			@Override
			public int size() {
				return b1.size() + b2.size();
			}
		};
	}

	private static ByteSequence concat3(ByteSequence b1, ByteSequence b2, ByteSequence b3) {
		return new ByteSequence() {
			@Override
			public ByteBuffer writeTo(ByteBuffer bb) {
				b1.writeTo(bb);
				b2.writeTo(bb);
				b3.writeTo(bb);
				return bb;
			}

			@Override
			public int size() {
				return b1.size() + b2.size() + b3.size();
			}
		};
	}

	static void addFilters(Scan scan, List<Filter> filters) {
		int n = filters.size();
		if (n == 1) {
			scan.setFilter(filters.get(0));
		} else if (n > 1) {
			scan.setFilter(new FilterList(filters));
		}
	}

	static byte[] prefixWithPartition(int partition, int nbits, ByteSequence bseq) {
		int nbytes = nbits/8;
		int rem = nbits - 8*nbytes;
		int numPartitions = (1 << nbits);
		if (partition >= numPartitions) {
			throw new IllegalArgumentException(String.format("Partition number %d must be less than %d (%d bits)", partition, numPartitions, nbits));
		}
		byte[] b = bseq.copyBytes();
		if (b.length < nbytes + (rem > 0 ? 1 : 0)) {
			throw new IllegalArgumentException(String.format("Byte array not long enough to hold %d bits", nbits));
		}
		for (int i=0; i<nbytes; i++) {
			b[i] = (byte) (partition >> (nbits - 8*(i + 1)));
		}
		if (rem > 0) {
			int shift = 8 - rem;
			// zero top bits
			b[nbytes] &= (byte) ((1 << shift) - 1);
			// replace top bits
			b[nbytes] |= (byte) (partition << shift);
		}
		return b;
	}

	private final Name name;
	final byte prefix;
	final RDFRole<T1> role1;
	final RDFRole<T2> role2;
	final RDFRole<T3> role3;
	final RDFRole<T4> role4;
	private final int[] argIndices;
	private final int[] spocIndices;
	private final RDFRole<?>[] spocRoles;
	private final RDFFactory rdfFactory;
	private final ValueIdentifier.Format idFormat;
	private final int cardinality1;
	private final int cardinality2;
	private final int cardinality3;
	private final int cardinality4;
	private final int maxCaching;

	StatementIndex(Name name, int prefix, RDFRole<T1> role1, RDFRole<T2> role2, RDFRole<T3> role3, RDFRole<T4> role4, RDFFactory rdfFactory, Configuration conf) {
		this.name = name;
		this.prefix = (byte) prefix;
		this.role1 = role1;
		this.role2 = role2;
		this.role3 = role3;
		this.role4 = role4;
		this.rdfFactory = rdfFactory;
		this.idFormat = rdfFactory.idFormat;
		this.maxCaching = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);

		this.argIndices = new int[4];
		this.spocIndices = new int[4];
		this.spocRoles = new RDFRole<?>[4];
		RDFRole<?>[] roles = new RDFRole<?>[] {role1, role2, role3, role4};
		for (int i=0; i<roles.length; i++) {
			RDFRole<?> role = roles[i];
			int spocIndex = role.getName().ordinal();
			this.argIndices[i] = spocIndex;
			this.spocIndices[spocIndex] = i;
			this.spocRoles[spocIndex] = role;
		}
		this.cardinality1 = getRoleCardinality(role1.getName());
		this.cardinality2 = getRoleCardinality(role2.getName());
		this.cardinality3 = getRoleCardinality(role3.getName());
		this.cardinality4 = getRoleCardinality(role4.getName());
	}

	public Name getName() {
		return name;
	}

	public RDFRole<?> getRole(RDFRole.Name name) {
		return spocRoles[name.ordinal()];
	}

	byte[] row(RDFIdentifier<T1> v1, RDFIdentifier<T2> v2, RDFIdentifier<T3> v3, @Nullable RDFIdentifier<T4> v4) {
		boolean hasQuad = (v4 != null);
		if (name.isQuadIndex() && !hasQuad) {
			throw new NullPointerException("Missing identifier from quad.");
		}
		byte[] r = new byte[1 + role1.keyHashSize() + role2.keyHashSize() + role3.keyHashSize() + (hasQuad ? role4.keyHashSize() : 0)];
		ByteBuffer bb = ByteBuffer.wrap(r);
		bb.put(prefix);
		bb.put(role1.keyHash(v1.getId(), idFormat));
		bb.put(role2.keyHash(v2.getId(), idFormat));
		bb.put(role3.keyHash(v3.getId(), idFormat));
		if(hasQuad) {
			bb.put(role4.keyHash(v4.getId(), idFormat));
		}
		return r;
	}

	byte[] qualifier(RDFIdentifier<T1> v1, @Nullable RDFIdentifier<T2> v2, @Nullable RDFIdentifier<T3> v3, @Nullable RDFIdentifier<T4> v4) {
		byte[] cq = new byte[role1.qualifierHashSize() + (v2 != null ? role2.qualifierHashSize() : 0) + (v3 != null ? role3.qualifierHashSize() : 0) + (v4 != null ? role4.qualifierHashSize() : 0)];
		ByteBuffer bb = ByteBuffer.wrap(cq);
		role1.writeQualifierHashTo(v1.getId(), bb);
		if(v2 != null) {
			role2.writeQualifierHashTo(v2.getId(), bb);
    		if(v3 != null) {
    			role3.writeQualifierHashTo(v3.getId(), bb);
        		if(v4 != null) {
					role4.writeQualifierHashTo(v4.getId(), bb);
        		}
    		}
		}
		return cq;
	}

	byte[] value(RDFValue<?,T1> v1, RDFValue<?,T2> v2, RDFValue<?,T3> v3, @Nullable RDFValue<?,T4> v4) {
		boolean hasQuad = (v4 != null);
		if (name.isQuadIndex() && !hasQuad) {
			throw new NullPointerException("Missing value from quad.");
		}
		int sizeLen1 = role1.sizeLength();
		int sizeLen2 = role2.sizeLength();
		int sizeLen3 = role3.sizeLength();
		byte[] cv = new byte[valueSize(v1, sizeLen1) + valueSize(v2, sizeLen2) + valueSize(v3, sizeLen3) + (hasQuad ? valueSize(v4, 0) : 0)];
		ByteBuffer bb = ByteBuffer.wrap(cv);
		putRDFValue(bb, v1, sizeLen1);
		putRDFValue(bb, v2, sizeLen2);
		putRDFValue(bb, v3, sizeLen3);
		if (hasQuad) {
			putLastRDFValue(bb, v4);
		}
		return cv;
	}

	Statement parseStatement(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx, ByteBuffer key, ByteBuffer cn, ByteBuffer cv, ValueFactory vf) {
		RDFValue<?,?>[] args = new RDFValue<?,?>[] {subj, pred, obj, ctx};
		Value v1 = parseRDFValue(role1, args[argIndices[0]], key, cn, cv, role1.keyHashSize(), vf);
		Value v2 = parseRDFValue(role2, args[argIndices[1]], key, cn, cv, role2.keyHashSize(), vf);
		Value v3 = parseRDFValue(role3, args[argIndices[2]], key, cn, cv, role3.keyHashSize(), vf);
		Value v4 = parseLastRDFValue(role4, args[argIndices[3]], key, cn, cv, role4.keyHashSize(), vf);
		return createStatement(new Value[] {v1, v2, v3, v4}, vf);
	}

    private Statement createStatement(Value[] vArray, ValueFactory vf) {
    	Resource s = (Resource) vArray[spocIndices[0]];
    	IRI p = (IRI) vArray[spocIndices[1]];
    	Value o = vArray[spocIndices[2]];
    	Resource c = (Resource) vArray[spocIndices[3]];
    	if (c == null) {
			return vf.createStatement(s, p, o);
		} else {
			return vf.createStatement(s, p, o, c);
		}
    }

    private Value parseRDFValue(RDFRole<?> role, @Nullable RDFValue<?,?> pattern, ByteBuffer key, ByteBuffer cq, ByteBuffer cv, int keySize, ValueFactory vf) {
    	byte marker = cv.get(cv.position()); // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
			switch (role.sizeLength()) {
				case Short.BYTES:
		    		len = cv.getShort();
					break;
				case Integer.BYTES:
		    		len = cv.getInt();
					break;
				default:
					throw new AssertionError(String.format("Unsupported size length: %d", role.sizeLength()));
			}
    	}
   		return parseValue(role, pattern, key, cq, cv, keySize, len, vf);
    }

    private Value parseLastRDFValue(RDFRole<?> role, @Nullable RDFValue<?,?> pattern, ByteBuffer key, ByteBuffer cq, ByteBuffer cv, int keySize, ValueFactory vf) {
    	byte marker = cv.hasRemaining() ? cv.get(cv.position()) : 0;  // peek
    	int len;
    	if (marker == WELL_KNOWN_IRI_MARKER) {
    		len = cv.get();
    	} else {
    		len = cv.remaining();
    	}
   		return parseValue(role, pattern, key, cq, cv, keySize, len, vf);
    }

	private Value parseValue(RDFRole<?> role, @Nullable RDFValue<?,?> pattern, ByteBuffer key, ByteBuffer cq, ByteBuffer cv, int keySize, int len, ValueFactory vf) {
    	if(pattern != null) {
    		// if we have been given the value then don't bother to read it and skip to the next
    		skipId(key, cq, keySize, rdfFactory.idFormat.size);
    		if (len > 0) {
    			cv.position(cv.position() + len);
    		}
			return pattern.val;
    	} else if(len == WELL_KNOWN_IRI_MARKER) {
			ValueIdentifier id = parseId(role, key, cq, keySize);
			IRI iri = rdfFactory.getWellKnownIRI(id);
			if (iri == null) {
				throw new IllegalStateException(String.format("Unknown IRI hash: %s (index %s, role %s)", id, getName(), role.getName()));
			}
			return iri;
		} else if(len > 0) {
			ValueIdentifier id = parseId(role, key, cq, keySize);
			int startPos = cv.position();
			int endPos = startPos + len;
			int prevLimit = cv.limit();
			cv.limit(endPos);
			ValueType valueType = rdfFactory.valueReader.getValueType(cv);
			Value value;
			if (valueType != null) {
				IdentifiableValue idValue;
				byte[] serBytes = new byte[len];
				cv.get(serBytes);
				ByteArray ser = new ByteArray(serBytes);
				switch (valueType) {
					case IRI:
						idValue = new IdentifiableIRI(ser, rdfFactory);
						break;
					case LITERAL:
						idValue = new IdentifiableLiteral(ser, rdfFactory);
						break;
					case BNODE:
						idValue = new IdentifiableBNode(ser, rdfFactory);
						break;
					case TRIPLE:
						idValue = new IdentifiableTriple(ser, rdfFactory);
						break;
					default:
						throw new AssertionError("Unexpected ValueType: " + valueType);
				}
				idValue.setId(id, rdfFactory);
				value = idValue;
			} else {
				value = rdfFactory.valueReader.readValue(cv, vf);
			}
			cv.limit(prevLimit);
			return value;
		} else if(len == 0) {
			return null;
		} else {
			throw new AssertionError(String.format("Invalid RDF value length: %d", len));
		}
    }

	private ValueIdentifier parseId(RDFRole<?> role, ByteBuffer key, ByteBuffer cn, int keySize) {
		byte[] idBytes = new byte[rdfFactory.idFormat.size];
		rdfFactory.idFormat.unrotate(key.array(), key.arrayOffset() + key.position(), keySize, role.getByteShift(), idBytes);
		key.position(key.position()+keySize);
		cn.get(idBytes, keySize, idBytes.length - keySize);
		return rdfFactory.id(idBytes);
	}

	Scan scan(ByteSequence k1Start, ByteSequence k2Start, ByteSequence k3Start, ByteSequence k4Start, ByteSequence k1Stop, ByteSequence k2Stop, ByteSequence k3Stop, ByteSequence k4Stop, int cardinality, boolean indiscriminate) {
        int rowBatchSize = HalyardTableUtils.rowBatchSize(cardinality, maxCaching);
		return HalyardTableUtils.scan(concat(false, k1Start, k2Start, k3Start, k4Start), concat(true, k1Stop, k2Stop, k3Stop, k4Stop), rowBatchSize, indiscriminate);
	}

	public Scan scan() {
		return scanWithConstraint(0, 0, null, null, null, null);
	}
	public Scan scanWithConstraint(int partition, int nbits, @Nullable ValueConstraint constraint1, @Nullable RDFIdentifier<T2> k2, @Nullable RDFIdentifier<T3> k3, @Nullable RDFIdentifier<T4> k4) {
		int cardinality = cardinality1*cardinality2*cardinality3*cardinality4;
		ByteSequence start1 = role1.startKey();
		ByteSequence stop1 = role1.stopKey();
		if (nbits > 0) {
			start1 = new ByteArray(prefixWithPartition(partition, nbits, start1));
			stop1 = new ByteArray(prefixWithPartition(partition, nbits, stop1));
		}
		Scan scan = scan(
			start1, role2.startKey(), role3.startKey(), role4.startKey(),
			stop1,  role2.stopKey(),  role3.stopKey(),  role4.stopKey(),
			cardinality,
			true
		);
		List<Filter> filters = new ArrayList<>();
		if (constraint1 != null) {
			appendValueConstraintFilters(ByteSequence.EMPTY, null,
				start1, stop1,
				concat3(role2.startKey(), role3.startKey(), role4.startKey()),
				concat3(role2.stopKey(),  role3.stopKey(),  role4.stopKey()),
				constraint1, filters);
		}
		if (k2 != null) {
			filters.add(createKeyFilter(role2.keyHash(k2.getId(), idFormat), 1 + role1.keyHashSize()));
		}
		if (k3 != null) {
			filters.add(createKeyFilter(role3.keyHash(k3.getId(), idFormat), 1 + role1.keyHashSize() + role2.keyHashSize()));
		}
		if (name.isQuadIndex() && k4 != null) {
			filters.add(createKeyFilter(role4.keyHash(k4.getId(), idFormat), 1 + role1.keyHashSize() + role2.keyHashSize() + role3.keyHashSize()));
		}
		addFilters(scan, filters);
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k) {
		return scanWithConstraint(k, 0, 0, null, null, null);
	}
	public Scan scanWithConstraint(RDFIdentifier<T1> k1, int partition, int nbits, @Nullable ValueConstraint constraint2, @Nullable RDFIdentifier<T3> k3, @Nullable RDFIdentifier<T4> k4) {
		ByteSequence kb = new ByteArray(role1.keyHash(k1.getId(), idFormat));
		int cardinality = cardinality1*cardinality2*cardinality3;
		ByteSequence start2 = role2.startKey();
		ByteSequence stop2 = role2.stopKey();
		if (nbits > 0) {
			start2 = new ByteArray(prefixWithPartition(partition, nbits, start2));
			stop2 = new ByteArray(prefixWithPartition(partition, nbits, stop2));
		}
		Scan scan = scan(
			kb, start2, role3.startKey(), role4.startKey(),
			kb, stop2,  role3.stopKey(),  role4.stopKey(),
			cardinality,
			false
		);
		List<Filter> filters = new ArrayList<>();
		filters.add(new ColumnPrefixFilter(qualifier(k1, null, null, null)));
		if (constraint2 != null) {
			appendValueConstraintFilters(kb, null,
				start2, stop2,
				concat2(role3.startKey(), role4.startKey()),
				concat2(role3.stopKey(),  role4.stopKey()),
				constraint2, filters);
		}
		if (k3 != null) {
			filters.add(createKeyFilter(role3.keyHash(k3.getId(), idFormat), 1 + role1.keyHashSize() + role2.keyHashSize()));
		}
		if (name.isQuadIndex() && k4 != null) {
			filters.add(createKeyFilter(role4.keyHash(k4.getId(), idFormat), 1 + role1.keyHashSize() + role2.keyHashSize() + role3.keyHashSize()));
		}
		addFilters(scan, filters);
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2) {
		return scanWithConstraint(k1, k2, 0, 0, null, null);
	}
	public Scan scanWithConstraint(RDFIdentifier<T1> k1, @Nullable RDFIdentifier<T2> k2, int partition, int nbits, @Nullable ValueConstraint constraint3, @Nullable RDFIdentifier<T4> k4) {
		ByteSequence k1b = new ByteArray(role1.keyHash(k1.getId(), idFormat));
		ByteSequence k2b, stop2;
		int cardinality;
		if (k2 != null) {
			k2b = new ByteArray(role2.keyHash(k2.getId(), idFormat));
			stop2 = k2b;
			cardinality = cardinality1*cardinality2;
		} else {
			k2b = role2.startKey();
			stop2 = role2.stopKey();
			cardinality = cardinality1*cardinality2*cardinality3;
		}
		ByteSequence start3 = role3.startKey();
		ByteSequence stop3 = role3.stopKey();
		if (nbits > 0) {
			start3 = new ByteArray(prefixWithPartition(partition, nbits, start3));
			stop3 = new ByteArray(prefixWithPartition(partition, nbits, stop3));
		}
		Scan scan = scan(
			k1b, k2b, start3, role4.startKey(),
			k1b, stop2, stop3, role4.stopKey(),
			cardinality,
			false
		);
		List<Filter> filters = new ArrayList<>();
		filters.add(new ColumnPrefixFilter(qualifier(k1, k2, null, null)));
		if (constraint3 != null) {
			appendValueConstraintFilters(concat2(k1b, k2b), k2 == null ? concat2(k1b, stop2) : null,
				start3, stop3,
				role4.startKey(), role4.stopKey(),
				constraint3, filters);
		}
		if (name.isQuadIndex() && k4 != null) {
			filters.add(createKeyFilter(role4.keyHash(k4.getId(), idFormat), 1 + role1.keyHashSize() + role2.keyHashSize() + role3.keyHashSize()));
		}
		addFilters(scan, filters);
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, RDFIdentifier<T3> k3) {
		return scanWithConstraint(k1, k2, k3, 0, 0, null);
	}
	public Scan scanWithConstraint(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, @Nullable RDFIdentifier<T3> k3, int partition, int nbits, @Nullable ValueConstraint constraint4) {
		ByteSequence k1b = new ByteArray(role1.keyHash(k1.getId(), idFormat));
		ByteSequence k2b = new ByteArray(role2.keyHash(k2.getId(), idFormat));
		ByteSequence k3b, stop3;
		int cardinality;
		if (k3 != null) {
			k3b = new ByteArray(role3.keyHash(k3.getId(), idFormat));
			stop3 = k3b;
			cardinality = cardinality1;
		} else {
			k3b = role3.startKey();
			stop3 = role3.stopKey();
			cardinality = cardinality1*cardinality2;
		}
		ByteSequence start4 = role4.startKey();
		ByteSequence stop4 = role4.stopKey();
		if (nbits > 0) {
			start4 = new ByteArray(prefixWithPartition(partition, nbits, start4));
			stop4 = new ByteArray(prefixWithPartition(partition, nbits, stop4));
		}
		Scan scan = scan(
			k1b, k2b, k3b, start4,
			k1b, k2b, stop3, stop4,
			cardinality,
			false
		);
		List<Filter> filters = new ArrayList<>();
		filters.add(new ColumnPrefixFilter(qualifier(k1, k2, k3, null)));
		if (constraint4 != null) {
			appendValueConstraintFilters(concat3(k1b, k2b, k3b), k3 == null ? concat3(k1b, k2b, stop3) : null,
				start4, stop4,
				ByteSequence.EMPTY,
				ByteSequence.EMPTY,
				constraint4, filters);
		}
		addFilters(scan, filters);
		return scan;
	}
	public Scan scan(RDFIdentifier<T1> k1, RDFIdentifier<T2> k2, RDFIdentifier<T3> k3, RDFIdentifier<T4> k4) {
		byte[] k1b = role1.keyHash(k1.getId(), idFormat);
		byte[] k2b = role2.keyHash(k2.getId(), idFormat);
		byte[] k3b = role3.keyHash(k3.getId(), idFormat);
		byte[] k4b = role4.keyHash(k4.getId(), idFormat);
		int cardinality = 1;
		return scan(
			new ByteArray(k1b), new ByteArray(k2b), new ByteArray(k3b), new ByteArray(k4b),
			new ByteArray(k1b), new ByteArray(k2b), new ByteArray(k3b), new ByteArray(k4b),
			cardinality,
			false
		).setFilter(new ColumnPrefixFilter(qualifier(k1, k2, k3, k4)));
	}

	private void appendValueConstraintFilters(ByteSequence prefix, @Nullable ByteSequence stopPrefix, ByteSequence startKey, ByteSequence stopKey, ByteSequence trailingStartKeys, ByteSequence trailingStopKeys, ValueConstraint constraint, List<Filter> filters) {
		ValueType type = constraint.getValueType();
		IRI dt = null;
		if ((constraint instanceof LiteralConstraint)) {
			LiteralConstraint objConstraint = (LiteralConstraint) constraint;
			dt = objConstraint.getDatatype();
		}
		int typeSaltSize = rdfFactory.idFormat.getSaltSize();
		List<RowRange> ranges;
		if (stopPrefix == null) {
			ranges = new ArrayList<>(typeSaltSize);
			for (int i=0; i<typeSaltSize; i++) {
				byte[] startRow = concat(false, prefix, rdfFactory.writeSaltAndType(i, type, dt, startKey), trailingStartKeys); // inclusive
				byte[] stopRow = concat(true, prefix, rdfFactory.writeSaltAndType(i, type, dt, stopKey), trailingStopKeys); // exclusive
				ranges.add(new RowRange(startRow, true, stopRow, false));
			}
		} else {
			byte[] startRow = concat(false, prefix, rdfFactory.writeSaltAndType(0, type, dt, startKey), trailingStartKeys); // inclusive
			byte[] stopRow = concat(true, stopPrefix, rdfFactory.writeSaltAndType(typeSaltSize-1, type, dt, stopKey), trailingStopKeys); // exclusive
			ranges = Collections.<RowRange>singletonList(new RowRange(startRow, true, stopRow, false));
		}
		filters.add(new MultiRowRangeFilter(ranges));
	}

	Scan scanWithConstraint(RDFSubject subj, RDFPredicate pred, RDFObject obj, RDFContext ctx, RDFRole.Name role, int partition, int partitionBits, ValueConstraint constraint) {
		RDFValue<?,T1> v1 = role1.getValue(subj, pred, obj, ctx);
		RDFValue<?,T2> v2 = role2.getValue(subj, pred, obj, ctx);
		RDFValue<?,T3> v3 = role3.getValue(subj, pred, obj, ctx);
		RDFValue<?,T4> v4 = role4.getValue(subj, pred, obj, ctx);
		int i = spocIndices[role.ordinal()];
		switch (i) {
			case 0:
				return scanWithConstraint(partition, partitionBits, constraint, v2, v3, v4);
			case 1:
				return scanWithConstraint(v1, partition, partitionBits, constraint, v3, v4);
			case 2:
				return scanWithConstraint(v1, v2, partition, partitionBits, constraint, v4);
			case 3:
				return scanWithConstraint(v1, v2, v3, partition, partitionBits, constraint);
			default:
				throw new AssertionError();
		}
	}

	private <T extends SPOC<?>> Filter createKeyFilter(byte[] key, int offset) {
		BinaryComponentComparator cmp = new BinaryComponentComparator(key, offset);
		return new RowFilter(CompareOperator.EQUAL, cmp);
	}

	/**
     * Helper method concatenating key parts.
     * @param trailingZero boolean switch adding trailing zero to the resulting key
     * @param fragments variable number of the key fragments as ByteSequences
     * @return concatenated key as byte array
     */
    byte[] concat(boolean trailingZero, ByteSequence... fragments) {
        int totalLen = 1; // for prefix
        for (ByteSequence fr : fragments) {
            totalLen += fr.size();
        }
        byte[] res = new byte[trailingZero ? totalLen + 1 : totalLen];
        ByteBuffer bb = ByteBuffer.wrap(res);
        bb.put(prefix);
        for (ByteSequence fr : fragments) {
            fr.writeTo(bb);
        }
        return res;
    }

    boolean isInPartition(RDFValue<?,?> val, RDFRole.Name roleName, int partition, int partitionBits) {
    	RDFRole<?> role = getRole(roleName);
		byte[] start = prefixWithPartition(partition, partitionBits, role.startKey());
		byte[] stop = prefixWithPartition(partition, partitionBits, role.stopKey());
		byte[] key = role.keyHash(val.getId(), idFormat);
		return Bytes.compareTo(start, key) <= 0 && Bytes.compareTo(key, stop) < 0;
    }

    @Override
	public String toString() {
		return name.toString();
	}
}
