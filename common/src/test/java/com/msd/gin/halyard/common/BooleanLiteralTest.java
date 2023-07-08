package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;

public class BooleanLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() {
		return new BooleanLiteral(true);
	}

	@Override
	protected Literal createOtherLiteral() {
		return new BooleanLiteral(false);
	}
}
