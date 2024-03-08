package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;
import org.junit.jupiter.api.Test;

public class XMLLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>";
		return new XMLLiteral(xml);
	}

	@Override
	protected Literal createOtherLiteral() {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test>foobar</test>";
		return new XMLLiteral(xml);
	}

	@Test
	public void testDocument() {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>";
		XMLLiteral l = new XMLLiteral(xml);
		l.objectValue();
	}
}
