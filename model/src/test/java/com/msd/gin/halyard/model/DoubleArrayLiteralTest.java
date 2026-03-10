package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.junit.jupiter.api.Test;

public class DoubleArrayLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() throws Exception {
		return new DoubleArrayLiteral(3.5, 2.1);
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		return new DoubleArrayLiteral(0.7, -3.7);
	}

	@Test
	public void testSameAsDifferentImpl() {
		DoubleArrayLiteral l1 = new DoubleArrayLiteral(3.5, 2.1);
		ObjectArrayLiteral l2 = new ObjectArrayLiteral(3.5, 2.1);
		assertEquals(l1, l2);
	}

	@Test
	public void testParse() {
		AbstractArrayLiteral<?> arr = AbstractArrayLiteral.create("[2.132315731412, 73.142349]");
		assertArrayEquals(new Object[] {2.132315731412, 73.142349}, arr.elements());
		assertInstanceOf(DoubleArrayLiteral.class, arr);
	}

	@Test
	public void testFromValues() {
		AbstractArrayLiteral<?> arr = AbstractArrayLiteral.createFromValues(new Value[] {valueFactory.createLiteral(2.132315731412), valueFactory.createLiteral(73.142349)});
		assertArrayEquals(new Object[] {2.132315731412, 73.142349}, arr.elements());
		assertInstanceOf(DoubleArrayLiteral.class, arr);
	}
}
