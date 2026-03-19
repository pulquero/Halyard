package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;

public class Base64LiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() {
		String b64 = "Zm9vYmFy";
		return new Base64Literal(b64);
	}

	@Override
	protected Literal createOtherLiteral() {
		byte[] bytes = new byte[] {1,5,3};
		return new Base64Literal(bytes);
	}
}
