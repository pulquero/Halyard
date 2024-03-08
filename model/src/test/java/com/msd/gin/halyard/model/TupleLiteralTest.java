package com.msd.gin.halyard.model;

import com.msd.gin.halyard.model.TupleLiteral;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TupleLiteralTest extends AbstractCustomLiteralTest {
	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	@Override
	protected Literal createLiteral() throws Exception {
		return new TupleLiteral(VF.createIRI("http://whatever"), VF.createLiteral(5), VF.createLiteral("foo"));
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		return new TupleLiteral(VF.createLiteral("foo", "en"), VF.createIRI("http://whatever"), VF.createLiteral(5));
	}

	@Test
	public void testParseIri() throws Exception {
		TupleLiteral expected = new TupleLiteral(VF.createIRI("http://whatever"), VF.createLiteral(true));
		String s = expected.stringValue();
		TupleLiteral actual = new TupleLiteral(s);
		assertEquals(expected, actual);
	}

	@Test
	public void testParseString() throws Exception {
		TupleLiteral expected = new TupleLiteral(VF.createLiteral("foo bar"), VF.createLiteral(true));
		String s = expected.stringValue();
		TupleLiteral actual = new TupleLiteral(s);
		assertEquals(expected, actual);
	}

	@Test
	public void testParseLangString() throws Exception {
		TupleLiteral expected = new TupleLiteral(VF.createLiteral("foo bar", "en"), VF.createLiteral(true));
		String s = expected.stringValue();
		TupleLiteral actual = new TupleLiteral(s);
		assertEquals(expected, actual);
	}

	@Test
	public void testParseDatatype() throws Exception {
		TupleLiteral expected = new TupleLiteral(VF.createLiteral(5), VF.createLiteral(true));
		String s = expected.stringValue();
		TupleLiteral actual = new TupleLiteral(s);
		assertEquals(expected, actual);
	}
}
