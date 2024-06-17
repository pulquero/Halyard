package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;

import org.eclipse.rdf4j.model.Literal;
import org.junit.jupiter.api.Test;

public class ArrayLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() throws Exception {
		return new ArrayLiteral(new int[] {8, 5});
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		return new ArrayLiteral(new Object[] {"foo", "bar"});
	}

	@Test
	public void testNestedArrays() {
		ArrayLiteral l = new ArrayLiteral(new Object[] {"abc", new Object[] {"a", "b", "c"}});
		assertEquals("[\"abc\",[\"a\",\"b\",\"c\"]]", l.getLabel());
	}

	@Test
	public void testNestedMap() {
		ArrayLiteral l = new ArrayLiteral(new Object[] {"abc", Collections.singletonMap("key", "value")});
		assertEquals("[\"abc\",{\"key\":\"value\"}]", l.getLabel());
	}
}
