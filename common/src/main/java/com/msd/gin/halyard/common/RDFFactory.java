package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class RDFFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(RDFFactory.class);
	private static final int MIN_KEY_SIZE = 1;
	private static final Map<HalyardTableConfiguration,RDFFactory> FACTORIES = Collections.synchronizedMap(new HashMap<>());

	private final HalyardTableConfiguration halyardConfig;
	public final ValueIO.Writer streamWriter;
	public final ValueIO.Reader streamReader;
	private final BiMap<ValueIdentifier, IRI> wellKnownIriIds = HashBiMap.create(256);
	final int version;
	final ValueIdentifier.Format idFormat;
	final int typeSaltSize;
	final IndexKeySizes spoKeySizes;
	final IndexKeySizes posKeySizes;
	final IndexKeySizes ospKeySizes;
	final IndexKeySizes cspoKeySizes;
	final IndexKeySizes cposKeySizes;
	final IndexKeySizes cospKeySizes;

	final ValueIO valueIO;

	static final class IndexKeySizes {
		int subjectSize;
		int predicateSize;
		int objectSize;
		int contextSize;

		IndexKeySizes(int subjectSize, int predicateSize, int objectSize, int contextSize) {
			this.subjectSize = subjectSize;
			this.predicateSize = predicateSize;
			this.objectSize = objectSize;
			this.contextSize = contextSize;
		}

		void readFrom(HalyardTableConfiguration conf, String prefix) {
			subjectSize = conf.getInt(prefix + ".subject.size", subjectSize);
			predicateSize = conf.getInt(prefix + ".predicate.size", predicateSize);
			objectSize = conf.getInt(prefix + ".object.size", objectSize);
			contextSize = conf.getInt(prefix + ".context.size", contextSize);
		}
	}


	private static RDFFactory create(HalyardTableConfiguration halyardConfig) {
		return FACTORIES.computeIfAbsent(halyardConfig, c -> new RDFFactory(halyardConfig));
	}

	public static RDFFactory create(Configuration config) {
		return create(new HalyardTableConfiguration(config));
	}

	public static RDFFactory create(KeyspaceConnection conn) throws IOException {
		Get getConfig = new Get(HalyardTableUtils.CONFIG_ROW_KEY)
				.addColumn(HalyardTableUtils.CF_NAME, HalyardTableUtils.CONFIG_COL);
		Result res = conn.get(getConfig);
		if (res == null) {
			throw new IOException("No config found");
		}
		Cell[] cells = res.rawCells();
		if (cells == null || cells.length == 0) {
			throw new IOException("No config found");
		}
		Cell cell = cells[0];
		Configuration halyardConf = new Configuration(false);
		ByteArrayInputStream bin = new ByteArrayInputStream(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
		halyardConf.addResource(bin);

		HalyardTableConfiguration config;
		// migrate old config
		int version = halyardConf.getInt(TableConfig.TABLE_VERSION, 0);
		if (version < TableConfig.CURRENT_VERSION && (conn instanceof TableKeyspace.TableKeyspaceConnection)) {
			halyardConf.setInt(TableConfig.TABLE_VERSION, TableConfig.CURRENT_VERSION);
			config = new HalyardTableConfiguration(halyardConf);
			HalyardTableUtils.writeConfig(((TableKeyspace.TableKeyspaceConnection)conn).getTable(), config);
		} else {
			config = new HalyardTableConfiguration(halyardConf);
		}
		return create(config);
	}

	private static int lessThan(int x, int upperLimit) {
		if (x >= upperLimit) {
			throw new IllegalArgumentException(String.format("%d must be less than %d", x, upperLimit));
		}
		return x;
	}

	private static int lessThanOrEqual(int x, int upperLimit) {
		if (x > upperLimit) {
			throw new IllegalArgumentException(String.format("%d must be less than or equal to %d", x, upperLimit));
		}
		return x;
	}

	private static int greaterThanOrEqual(int x, int lowerLimit) {
		if (x < lowerLimit) {
			throw new IllegalArgumentException(String.format("%d must be greater than or equal to %d", x, lowerLimit));
		}
		return x;
	}

	private RDFFactory(HalyardTableConfiguration halyardConfig) {
		this.halyardConfig = halyardConfig;
		version = halyardConfig.getInt(TableConfig.TABLE_VERSION);
		if (version < TableConfig.VERSION_4_6_1) {
			throw new RuntimeException("Old table format - please reload your data");
		}
		if (version > TableConfig.CURRENT_VERSION) {
			throw new RuntimeException("New table format - please upgrade your installation");
		}
		valueIO = new ValueIO(halyardConfig);
		String confIdAlgo = halyardConfig.get(TableConfig.ID_HASH);
		int confIdSize = halyardConfig.getInt(TableConfig.ID_SIZE);
		int idSize = Hashes.getHash(confIdAlgo, confIdSize).size();
		LOGGER.info("Identifier hash: {} {}-bit ({} bytes)", confIdAlgo, idSize*Byte.SIZE, idSize);

		int typeIndex = lessThan(lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInt(TableConfig.ID_TYPE_INDEX), 0), Short.BYTES), idSize);
		ValueIdentifier.TypeNibble typeNibble = halyardConfig.getBoolean(TableConfig.ID_TYPE_NIBBLE) ? ValueIdentifier.TypeNibble.LITTLE_NIBBLE : ValueIdentifier.TypeNibble.BIG_NIBBLE;
		idFormat = new ValueIdentifier.Format(confIdAlgo, idSize, typeIndex, typeNibble);
		typeSaltSize = idFormat.getSaltSize();

		int subjectKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInt(TableConfig.KEY_SIZE_SUBJECT), MIN_KEY_SIZE), idSize);
		int subjectEndKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInt(TableConfig.END_KEY_SIZE_SUBJECT), MIN_KEY_SIZE), idSize);
		int predicateKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInt(TableConfig.KEY_SIZE_PREDICATE), MIN_KEY_SIZE), idSize);
		int predicateEndKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInt(TableConfig.END_KEY_SIZE_PREDICATE), MIN_KEY_SIZE), idSize);
		int objectKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInt(TableConfig.KEY_SIZE_OBJECT), MIN_KEY_SIZE), idSize);
		int objectEndKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInt(TableConfig.END_KEY_SIZE_OBJECT), MIN_KEY_SIZE), idSize);
		int contextKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInt(TableConfig.KEY_SIZE_CONTEXT), MIN_KEY_SIZE), idSize);
		int contextEndKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInt(TableConfig.END_KEY_SIZE_CONTEXT), 0), idSize);

		spoKeySizes = new IndexKeySizes(subjectKeySize, predicateKeySize, objectEndKeySize, contextEndKeySize);
		spoKeySizes.readFrom(halyardConfig, "halyard.key.spo");
		posKeySizes = new IndexKeySizes(subjectEndKeySize, predicateKeySize, objectKeySize, contextEndKeySize);
		posKeySizes.readFrom(halyardConfig, "halyard.key.pos");
		ospKeySizes = new IndexKeySizes(subjectKeySize, predicateEndKeySize, objectKeySize, contextEndKeySize);
		ospKeySizes.readFrom(halyardConfig, "halyard.key.osp");
		cspoKeySizes = new IndexKeySizes(subjectKeySize, predicateKeySize, objectEndKeySize, contextKeySize);
		cspoKeySizes.readFrom(halyardConfig, "halyard.key.cspo");
		cposKeySizes = new IndexKeySizes(subjectEndKeySize, predicateKeySize, objectKeySize, contextKeySize);
		cposKeySizes.readFrom(halyardConfig, "halyard.key.cpos");
		cospKeySizes = new IndexKeySizes(subjectKeySize, predicateEndKeySize, objectKeySize, contextKeySize);
		cospKeySizes.readFrom(halyardConfig, "halyard.key.cosp");

		streamWriter = valueIO.createStreamWriter();
		streamReader = valueIO.createStreamReader(IdValueFactory.INSTANCE);

		for (IdentifiableIRI iri : halyardConfig.getWellKnownIRIs()) {
			ValueIdentifier id = iri.getId(this);
			if (wellKnownIriIds.putIfAbsent(id, iri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						wellKnownIriIds.get(id), iri));
			}
		}
	}

	public Collection<? extends Namespace> getWellKnownNamespaces() {
		return halyardConfig.getWellKnownNamespaces();
	}

	IRI getWellKnownIRI(ValueIdentifier id) {
		return wellKnownIriIds.get(id);
	}

	boolean isWellKnownIRI(Value v) {
		return v.isIRI() && wellKnownIriIds.containsValue(v);
	}

	public int getIdSize() {
		return idFormat.size;
	}

	public String getIdAlgorithm() {
		return idFormat.algorithm;
	}

	ByteSequence writeSaltAndType(final int salt, ValueType type, IRI datatype, ByteSequence seq) {
		if (salt >= typeSaltSize) {
			throw new IllegalArgumentException(String.format("Salt must be between 0 (inclusive) and %d (exclusive): %d", typeSaltSize, salt));
		}
		return new ByteSequence() {
			@Override
			public ByteBuffer writeTo(ByteBuffer bb) {
				int saltBits = salt;
				byte[] arr = bb.array();
				int offset = bb.arrayOffset() + bb.position();
				seq.writeTo(bb);
				// overwrite type bits
				idFormat.writeType(type, datatype, arr, offset);
				if (idFormat.typeNibble == ValueIdentifier.TypeNibble.LITTLE_NIBBLE) {
					arr[offset+idFormat.typeIndex] = (byte) ((arr[offset+idFormat.typeIndex] & 0x0F) | ((saltBits&0x0F) << 4));
					saltBits >>= 4;
				}
				for (int i=idFormat.typeIndex-1; i>=0; i--) {
					arr[offset+i] = (byte) saltBits;
					saltBits >>= 8;
				}
				return bb;
			}

			@Override
			public int size() {
				return seq.size();
			}
		};
	}

	private IndexKeySizes getKeySizes(StatementIndex.Name index) {
		switch(index) {
			case SPO:
				return spoKeySizes;
			case POS:
				return posKeySizes;
			case OSP:
				return ospKeySizes;
			case CSPO:
				return cspoKeySizes;
			case CPOS:
				return cposKeySizes;
			case COSP:
				return cospKeySizes;
			default:
				throw new AssertionError();
		}
	}

	RDFRole<SPOC.S> getSubjectRole(StatementIndex.Name index) {
		return new RDFRole<>(
			RDFRole.Name.SUBJECT,
			idFormat.size,
			getKeySizes(index).subjectSize,
			getSubjectShift(index), Short.BYTES,
			true
		);
	}

	private int getSubjectShift(StatementIndex.Name index) {
		switch(index) {
			case SPO:
			case CSPO:
				return 0;
			case POS:
			case CPOS:
				return 2;
			case OSP:
			case COSP:
				return 1;
			default:
				throw new AssertionError();
		}
	}

	RDFRole<SPOC.P> getPredicateRole(StatementIndex.Name index) {
		return new RDFRole<>(
			RDFRole.Name.PREDICATE,
			idFormat.size,
			getKeySizes(index).predicateSize,
			getPredicateShift(index), Short.BYTES,
			true
		);
	}

	private int getPredicateShift(StatementIndex.Name index) {
		switch(index) {
			case SPO:
			case CSPO:
				return 1;
			case POS:
			case CPOS:
				return 0;
			case OSP:
			case COSP:
				return 2;
			default:
				throw new AssertionError();
		}
	}

	RDFRole<SPOC.O> getObjectRole(StatementIndex.Name index) {
		return new RDFRole<>(
			RDFRole.Name.OBJECT,
			idFormat.size,
			getKeySizes(index).objectSize,
			getObjectShift(index), Integer.BYTES,
			true
		);
	}

	private int getObjectShift(StatementIndex.Name index) {
		switch(index) {
			case SPO:
			case CSPO:
				return 2;
			case POS:
			case CPOS:
				return 1;
			case OSP:
			case COSP:
				return 0;
			default:
				throw new AssertionError();
		}
	}

	RDFRole<SPOC.C> getContextRole(StatementIndex.Name index) {
		return new RDFRole<>(
			RDFRole.Name.CONTEXT,
			idFormat.size,
			getKeySizes(index).contextSize,
			0, Short.BYTES,
			index.isQuadIndex()
		);
	}

	public ValueIdentifier id(Value v) {
		ValueIdentifier id = v.isIRI() ? wellKnownIriIds.inverse().get(v) : null;
		if (id == null) {
			if (v instanceof IdentifiableValue) {
				IdentifiableValue idv = (IdentifiableValue) v;
				id = idv.getId(this);
			} else {
				ByteBuffer ser = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
				ser = streamWriter.writeTo(v, ser);
				ser.flip();
				id = id(v, ser);
			}
		}
		return id;
	}

	public ValueIdentifier id(byte[] idBytes) {
		if (idBytes.length != idFormat.size) {
			throw new IllegalArgumentException("Byte array has incorrect length");
		}
		return new ValueIdentifier(idBytes, idFormat);
	}

	ValueIdentifier id(Value v, ByteBuffer ser) {
		ValueType type = ValueType.valueOf(v);
		return idFormat.id(type, v.isLiteral() ? ((Literal)v).getDatatype() : null, ser);
	}

	public ValueIdentifier idFromString(String s) {
		return new ValueIdentifier(Hashes.decode(s), idFormat);
	}

	ByteBuffer getSerializedForm(Value v) {
		byte[] b = streamWriter.toBytes(v);
		return ByteBuffer.wrap(b).asReadOnlyBuffer();
	}

	public byte[] statementId(Resource subj, IRI pred, Value obj) {
		byte[] id = new byte[3 * idFormat.size];
		ByteBuffer buf = ByteBuffer.wrap(id);
		buf = writeStatementId(subj, pred, obj, buf);
		buf.flip();
		buf.get(id);
		return id;
	}

	public ByteBuffer writeStatementId(Resource subj, IRI pred, Value obj, ByteBuffer buf) {
		buf = ValueIO.ensureCapacity(buf, 3*idFormat.size);
		id(subj).writeTo(buf);
		id(pred).writeTo(buf);
		id(obj).writeTo(buf);
		return buf;
	}

	public RDFIdentifier createSubjectId(ValueIdentifier id) {
		return new RDFIdentifier(RDFRole.Name.SUBJECT, id);
	}

	public RDFIdentifier createPredicateId(ValueIdentifier id) {
		return new RDFIdentifier(RDFRole.Name.PREDICATE, id);
	}

	public RDFIdentifier createObjectId(ValueIdentifier id) {
		return new RDFIdentifier(RDFRole.Name.OBJECT, id);
	}

	public RDFSubject createSubject(Resource val) {
		return RDFSubject.create(val, this);
	}

	public RDFPredicate createPredicate(IRI val) {
		return RDFPredicate.create(val, this);
	}

	public RDFObject createObject(Value val) {
		return RDFObject.create(val, this);
	}

	public RDFContext createContext(Resource val) {
		return RDFContext.create(val, this);
	}

	/**
	 * Used for testing.
	 */
	ValueIO.Writer createWriter() {
		return valueIO.createWriter(null);
	}

	/**
	 * Used for testing.
	 */
	ValueIO.Reader createReader(ValueFactory vf) {
		return valueIO.createReader(vf, null);
	}
}
