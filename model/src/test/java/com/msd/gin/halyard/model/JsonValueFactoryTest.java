package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.model.vocabulary.HalyardDatatype;

public class JsonValueFactoryTest {
	private final ValueFactory valueFactory = SimpleValueFactory.getInstance();
	private final JsonValueFactory jsonvf = new JsonOrgJsonValueFactory(valueFactory);

	@Test
	public void testObject() {
		Literal l = jsonvf.createJsonLiteral("{\"foo\":\"bar\"}");
		assertEquals(HalyardDatatype.MAP, l.getCoreDatatype());
		assertEquals("bar", ((MapLiteral)l).objectValue().get("foo"));
	}

	@Test
	public void testDoubleArray() {
		Literal l = jsonvf.createJsonLiteral("[2.132315731412, 73.142349]");
		assertEquals(HalyardDatatype.ARRAY, l.getCoreDatatype());
		AbstractArrayLiteral<?> arr = (AbstractArrayLiteral<?>) l;
		assertArrayEquals(new Object[] {2.132315731412, 73.142349}, arr.elements());
		assertInstanceOf(DoubleArrayLiteral.class, arr);
	}

	@Test
	public void testDoubleArrayFromValues() {
		AbstractArrayLiteral<?> arr = AbstractArrayLiteral.createFromValues(new Value[] {valueFactory.createLiteral(2.132315731412), valueFactory.createLiteral(73.142349)});
		assertArrayEquals(new Object[] {2.132315731412, 73.142349}, arr.elements());
		assertInstanceOf(DoubleArrayLiteral.class, arr);
	}

	@Test
	public void testFloatArray() {
		Literal l = jsonvf.createJsonLiteral("[2.5, 7.25]");
		assertEquals(HalyardDatatype.ARRAY, l.getCoreDatatype());
		AbstractArrayLiteral<?> arr = (AbstractArrayLiteral<?>) l;
		assertArrayEquals(new Object[] {2.5f, 7.25f}, arr.elements());
		assertInstanceOf(FloatArrayLiteral.class, arr);
	}

	@Test
	public void testFloatArrayFromValues() {
		AbstractArrayLiteral<?> arr = AbstractArrayLiteral.createFromValues(new Value[] {valueFactory.createLiteral(2.5f), valueFactory.createLiteral(7.25f)});
		assertArrayEquals(new Object[] {2.5f, 7.25f}, arr.elements());
		assertInstanceOf(FloatArrayLiteral.class, arr);
	}

	@Test
	public void testStringArray() {
		Literal l = jsonvf.createJsonLiteral("[\"foo\", \"bar\"]");
		assertEquals(HalyardDatatype.ARRAY, l.getCoreDatatype());
		AbstractArrayLiteral<?> arr = (AbstractArrayLiteral<?>) l;
		assertArrayEquals(new Object[] {"foo", "bar"}, arr.elements());
		assertEquals(String.class, arr.componentType());
		assertInstanceOf(ObjectArrayLiteral.class, arr);
	}

	@Test
	public void testStringArrayFromValues() {
		AbstractArrayLiteral<?> arr = AbstractArrayLiteral.createFromValues(new Value[] {valueFactory.createLiteral("foo"), valueFactory.createLiteral("bar")});
		assertArrayEquals(new Object[] {"foo", "bar"}, arr.elements());
		assertEquals(String.class, arr.componentType());
		assertInstanceOf(ObjectArrayLiteral.class, arr);
	}
}
