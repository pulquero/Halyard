package com.msd.gin.halyard.common;

import com.google.common.collect.ImmutableMap;
import com.msd.gin.halyard.model.ValueType;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

/**
 * Test different RDFFactory configurations.
 */
@RunWith(Parameterized.class)
public class RDFFactoryExtendedTest {

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		Configuration littleNibbleConf = new Configuration();
		littleNibbleConf.setInt(TableConfig.ID_TYPE_INDEX, 0);
		littleNibbleConf.setBoolean(TableConfig.ID_TYPE_NIBBLE, true);
		littleNibbleConf.setInt(TableConfig.KEY_SIZE_SUBJECT, 3);
		littleNibbleConf.setInt(TableConfig.END_KEY_SIZE_SUBJECT, 4);
		littleNibbleConf.setInt(TableConfig.KEY_SIZE_CONTEXT, 4);
		littleNibbleConf.setInt(TableConfig.END_KEY_SIZE_CONTEXT, 2);

		Configuration littleNibbleMoreSaltConf = new Configuration();
		littleNibbleMoreSaltConf.setInt(TableConfig.ID_TYPE_INDEX, 1);
		littleNibbleMoreSaltConf.setBoolean(TableConfig.ID_TYPE_NIBBLE, true);
		littleNibbleMoreSaltConf.setInt(TableConfig.KEY_SIZE_SUBJECT, 3);
		littleNibbleMoreSaltConf.setInt(TableConfig.END_KEY_SIZE_SUBJECT, 4);
		littleNibbleMoreSaltConf.setInt(TableConfig.KEY_SIZE_CONTEXT, 4);
		littleNibbleMoreSaltConf.setInt(TableConfig.END_KEY_SIZE_CONTEXT, 2);

		Configuration bigNibbleConf = new Configuration();
		bigNibbleConf.setInt(TableConfig.ID_TYPE_INDEX, 1);
		bigNibbleConf.setBoolean(TableConfig.ID_TYPE_NIBBLE, false);
		bigNibbleConf.setInt(TableConfig.KEY_SIZE_SUBJECT, 4);
		bigNibbleConf.setInt(TableConfig.END_KEY_SIZE_SUBJECT, 2);
		bigNibbleConf.setInt(TableConfig.KEY_SIZE_CONTEXT, 4);
		bigNibbleConf.setInt(TableConfig.END_KEY_SIZE_CONTEXT, 3);

		Configuration namespaceConf = new Configuration();
		namespaceConf.setInt(TableConfig.KEY_SIZE_SUBJECT, 2);
		namespaceConf.setInt(TableConfig.END_KEY_SIZE_SUBJECT, 2);
		namespaceConf.setInt(TableConfig.KEY_SIZE_CONTEXT, 2);
		namespaceConf.setInt(TableConfig.END_KEY_SIZE_CONTEXT, 2);
		namespaceConf.set(TableConfig.VOCABS, JSONObject.valueToString(ImmutableMap.of(RDF.TYPE.stringValue(), 25)));
		namespaceConf.set(TableConfig.NAMESPACES, JSONObject.valueToString(ImmutableMap.of("http://whatever", 1)));
		namespaceConf.set(TableConfig.NAMESPACE_PREFIXES, JSONObject.valueToString(ImmutableMap.of("w", "http://whatever")));
		namespaceConf.set(TableConfig.LANGS, JSONObject.valueToString(ImmutableMap.of("en", 1)));

		return Arrays.<Object[]>asList(
			new Object[] {littleNibbleConf},
			new Object[] {littleNibbleMoreSaltConf},
			new Object[] {bigNibbleConf},
			new Object[] {namespaceConf}
		);
	}

	private final Configuration conf;
	private final RDFFactory rdfFactory;

	public RDFFactoryExtendedTest(Configuration conf) {
		this.conf = conf;
		this.rdfFactory = RDFFactory.create(conf);
	}

	@Test
	public void testUniqueEncodingTypeFlags() throws IllegalAccessException {
		Map<Byte,String> flags = new HashMap<>();
		for (Field f : ValueIO.class.getDeclaredFields()) {
			f.setAccessible(true);
			String fName = f.getName();
			if (fName.endsWith("_TYPE")) {
				byte flag = f.getByte(null);
				String oldName = flags.put(flag, fName);
				if (oldName != null) {
					throw new AssertionError(String.format("%s: %c already used by %s", fName, flag, oldName));
				}
			}
		}
	}

	@Test
	public void testKeySizes() {
		int subjEndKeySize = conf.getInt(TableConfig.END_KEY_SIZE_SUBJECT, 0);
		RDFRole<?> role = rdfFactory.getSubjectRole(StatementIndex.Name.POS);
		assertEquals(subjEndKeySize, role.keyHashSize());

		int ctxEndKeySize = conf.getInt(TableConfig.END_KEY_SIZE_CONTEXT, 0);
		role = rdfFactory.getContextRole(StatementIndex.Name.OSP);
		assertEquals(ctxEndKeySize, role.keyHashSize());

		role = rdfFactory.getSubjectRole(StatementIndex.Name.CPOS);
		assertEquals(subjEndKeySize, role.keyHashSize());

		int ctxKeySize = conf.getInt(TableConfig.KEY_SIZE_CONTEXT, 0);
		role = rdfFactory.getContextRole(StatementIndex.Name.COSP);
		assertEquals(ctxKeySize, role.keyHashSize());
	}

	@Test
	public void testWriteSaltAndTypeIRI() {
		ValueIdentifier.Format idFormat = rdfFactory.idFormat;
		Set<ByteBuffer> salts = new LinkedHashSet<>();
		int typeSaltSize = idFormat.getSaltSize();
		for (int i=0; i<typeSaltSize; i++) {
			byte[] idBytes = new byte[idFormat.size];
			ByteBuffer bb = ByteBuffer.wrap(idBytes);
			rdfFactory.writeSaltAndType(i, ValueType.IRI, null, new ByteFiller((byte)0xFF, idFormat.size)).writeTo(bb);
			assertFalse(bb.hasRemaining());
			bb.flip();
			salts.add(bb);
			ValueIdentifier id = rdfFactory.idFromBytes(idBytes);
			assertTrue(id.isIRI(idFormat));
		}
		assertEquals(typeSaltSize, salts.size());
	}

	@Test
	public void testWriteSaltAndTypeStringLiteral() {
		ValueIdentifier.Format idFormat = rdfFactory.idFormat;
		Set<ByteBuffer> salts = new LinkedHashSet<>();
		int typeSaltSize = idFormat.getSaltSize();
		for (int i=0; i<typeSaltSize; i++) {
			byte[] idBytes = new byte[idFormat.size];
			ByteBuffer bb = ByteBuffer.wrap(idBytes);
			rdfFactory.writeSaltAndType(i, ValueType.LITERAL, CoreDatatype.XSD.STRING, new ByteFiller((byte)0xFF, idFormat.size)).writeTo(bb);
			assertFalse(bb.hasRemaining());
			bb.flip();
			salts.add(bb);
			ValueIdentifier id = rdfFactory.idFromBytes(idBytes);
			assertTrue(id.isLiteral(idFormat));
			assertTrue(id.isString(idFormat));
		}
		assertEquals(typeSaltSize, salts.size());
	}

	@Test
	public void testIdIsEqual() {
        ValueFactory vf = SimpleValueFactory.getInstance();
        assertEquals(
	    	rdfFactory.id(vf.createLiteral("1", vf.createIRI("local:type"))),
	    	rdfFactory.id(vf.createLiteral("1", vf.createIRI("local:type")))
	    );
	}

	@Test
    public void testIdIsUnique() {
        ValueFactory vf = SimpleValueFactory.getInstance();
        List<Value> values = RDFFactoryTest.createData(vf).stream().map(arr -> (Value)arr[0]).collect(Collectors.toList());
        Set<ValueIdentifier> ids = new HashSet<>(values.size());
        for (Value v : values) {
        	ids.add(rdfFactory.id(v));
        }
        assertEquals(values.size(), ids.size());
    }

	@Test
	public void testWellKnownIRI() {
		assertTrue(rdfFactory.createIRI(RDF.TYPE.stringValue()).isWellKnown());
		ValueIdentifier id = rdfFactory.id(RDF.TYPE);
		IRI typeIri = rdfFactory.getWellKnownIRI(id);
		assertEquals(RDF.TYPE, typeIri);
	}

	@Test
	public void testNamespacePrefixes() {
		assertTrue(rdfFactory.getWellKnownNamespaces().size() > 0);
	}

	@Test
	public void testIsSingleton() {
		Configuration conf2 = new Configuration(conf);
		RDFFactory rdfFactory2 = RDFFactory.create(conf2);
		assertTrue(rdfFactory == rdfFactory2);
	}
}
