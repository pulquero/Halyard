package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.rdf4j.model.Literal;
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
}
