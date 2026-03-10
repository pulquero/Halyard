package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Collections;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.junit.jupiter.api.Test;

public class ObjectArrayLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() throws Exception {
		return new ObjectArrayLiteral(8, 5);
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		return new ObjectArrayLiteral("foo", "bar");
	}

	@Test
	public void testNestedArrays() {
		ObjectArrayLiteral l = new ObjectArrayLiteral(new Object[] {"abc", new Object[] {"a", "b", "c"}});
		assertEquals("[\"abc\",[\"a\",\"b\",\"c\"]]", l.getLabel());
	}

	@Test
	public void testNestedMap() {
		ObjectArrayLiteral l = new ObjectArrayLiteral(new Object[] {"abc", Collections.singletonMap("key", "value")});
		assertEquals("[\"abc\",{\"key\":\"value\"}]", l.getLabel());
	}

	@Test
	public void testSameAsDifferentImpl() {
		ObjectArrayLiteral l1 = new ObjectArrayLiteral(3.5f, 2.1f);
		FloatArrayLiteral l2 = new FloatArrayLiteral(3.5f, 2.1f);
		assertEquals(l1, l2);
	}

	@Test
	public void testParse() {
		AbstractArrayLiteral<?> arr = AbstractArrayLiteral.create("[\"foo\", \"bar\"]");
		assertArrayEquals(new Object[] {"foo", "bar"}, arr.elements());
		assertEquals(String.class, arr.componentType());
		assertInstanceOf(ObjectArrayLiteral.class, arr);
	}

	@Test
	public void testFromValues() {
		AbstractArrayLiteral<?> arr = AbstractArrayLiteral.createFromValues(new Value[] {valueFactory.createLiteral("foo"), valueFactory.createLiteral("bar")});
		assertArrayEquals(new Object[] {"foo", "bar"}, arr.elements());
		assertEquals(String.class, arr.componentType());
		assertInstanceOf(ObjectArrayLiteral.class, arr);
	}
}
