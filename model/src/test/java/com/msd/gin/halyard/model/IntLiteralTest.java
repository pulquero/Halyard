package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public class IntLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() {
		return new IntLiteral(56, CoreDatatype.XSD.INTEGER);
	}

	@Override
	protected Literal createOtherLiteral() {
		return new IntLiteral(56, CoreDatatype.XSD.INT);
	}
}
