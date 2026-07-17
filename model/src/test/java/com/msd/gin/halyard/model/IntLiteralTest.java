package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;

public class IntLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() {
		return IntLiteral.createInteger(56);
	}

	@Override
	protected Literal createOtherLiteral() {
		return IntLiteral.createInt(56);
	}
}
