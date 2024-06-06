package com.msd.gin.halyard.sail.search.function;

import com.msd.gin.halyard.model.ArrayLiteral;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class EscapeTermTest {
	private final ValueFactory vf = SimpleValueFactory.getInstance();

	@Test
	public void testNonReserved() {
		Literal escaped = (Literal) new EscapeTerm().evaluate(vf, vf.createLiteral("FooBar"));
		assertEquals("FooBar", escaped.stringValue());
	}

	@Test
	public void testRemoveReserved() {
		Literal escaped = (Literal) new EscapeTerm().evaluate(vf, vf.createLiteral("f<oo>bar"));
		assertEquals("foobar", escaped.stringValue());
	}

	@Test
	public void testLowerCaseOperator1() {
		Literal escaped = (Literal) new EscapeTerm().evaluate(vf, vf.createLiteral("foo AND bar"));
		assertEquals("foo and bar", escaped.stringValue());
	}

	@Test
	public void testLowerCaseOperator2() {
		Literal escaped = (Literal) new EscapeTerm().evaluate(vf, vf.createLiteral("OR"));
		assertEquals("or", escaped.stringValue());
	}

	@Test
	public void testIgnoreFakeOperator() {
		Literal escaped = (Literal) new EscapeTerm().evaluate(vf, vf.createLiteral("fooANDbar"));
		assertEquals("fooANDbar", escaped.stringValue());
	}

	@Test
	public void testEscapeReserved1() {
		Literal escaped = (Literal) new EscapeTerm().evaluate(vf, vf.createLiteral("f+oo!b/ar"));
		assertEquals("f\\+oo\\!b\\/ar", escaped.stringValue());
	}

	@Test
	public void testEscapeReserved2() {
		Literal escaped = (Literal) new EscapeTerm().evaluate(vf, vf.createLiteral("f&&oo||bar"));
		assertEquals("f\\&&oo\\||bar", escaped.stringValue());
	}

	@Test
	public void testEscapeUrl() {
		Literal escaped = (Literal) new EscapeTerm().evaluate(vf, vf.createLiteral("http://www.com/path"));
		assertEquals("http\\:\\/\\/www.com\\/path", escaped.stringValue());
	}

	@Test
	public void testEscapeList() {
		Literal escaped = (Literal) new EscapeTerm().evaluate(vf, new ArrayLiteral(":", "/"));
		assertEquals(new ArrayLiteral("\\:", "\\/"), escaped);
	}
}
