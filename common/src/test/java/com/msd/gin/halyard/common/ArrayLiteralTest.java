package com.msd.gin.halyard.common;

import java.util.Arrays;

import org.eclipse.rdf4j.model.Literal;

public class ArrayLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() throws Exception {
		return new ArrayLiteral(Arrays.asList(8, 5));
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		return new ArrayLiteral(Arrays.asList("foo", "bar"));
	}
}
