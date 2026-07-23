package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;

public class LongLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() {
		return LongLiteral.createInteger(56L);
	}

	@Override
	protected Literal createOtherLiteral() {
		return LongLiteral.createLong(56L);
	}
}
