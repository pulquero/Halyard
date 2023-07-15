package com.msd.gin.halyard.common;

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
	public void testParsing1() throws Exception {
		TupleLiteral expected = new TupleLiteral(VF.createIRI("http://whatever"), VF.createLiteral(5), VF.createLiteral("foo bar"));
		String s = expected.stringValue();
		TupleLiteral actual = new TupleLiteral(s);
		assertEquals(expected, actual);
	}

	@Test
	public void testParsing2() throws Exception {
		TupleLiteral expected = new TupleLiteral(VF.createIRI("http://whatever"), VF.createLiteral(5), VF.createLiteral("foo bar", "en"));
		String s = expected.stringValue();
		TupleLiteral actual = new TupleLiteral(s);
		assertEquals(expected, actual);
	}

	@Test
	public void testParsing3() throws Exception {
		TupleLiteral expected = new TupleLiteral(VF.createLiteral(true), VF.createIRI("http://whatever"));
		String s = expected.stringValue();
		TupleLiteral actual = new TupleLiteral(s);
		assertEquals(expected, actual);
	}
}
