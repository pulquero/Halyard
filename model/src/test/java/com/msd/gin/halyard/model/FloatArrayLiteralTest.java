package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.junit.jupiter.api.Test;

public class FloatArrayLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() throws Exception {
		return new FloatArrayLiteral(3.5f, 2.1f);
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		return new FloatArrayLiteral(0.7f, -3.7f);
	}

	@Test
	public void testSameAsDifferentImpl() {
		FloatArrayLiteral l1 = new FloatArrayLiteral(3.5f, 2.1f);
		ObjectArrayLiteral l2 = new ObjectArrayLiteral(3.5f, 2.1f);
		assertEquals(l1, l2);
	}

	@Test
	public void testParse() {
		AbstractArrayLiteral<?> arr = AbstractArrayLiteral.create("[2.5, 7.25]");
		assertArrayEquals(new Object[] {2.5f, 7.25f}, arr.elements());
		assertInstanceOf(FloatArrayLiteral.class, arr);
	}

	@Test
	public void testFromValues() {
		AbstractArrayLiteral<?> arr = AbstractArrayLiteral.createFromValues(new Value[] {valueFactory.createLiteral(2.5f), valueFactory.createLiteral(7.25f)});
		assertArrayEquals(new Object[] {2.5f, 7.25f}, arr.elements());
		assertInstanceOf(FloatArrayLiteral.class, arr);
	}
}
