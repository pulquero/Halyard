package com.msd.gin.halyard.sail.search;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class GroupTermsFunctionTest {
	private final ValueFactory vf = SimpleValueFactory.getInstance();

	@Test
	public void test() {
		Literal group = (Literal) new GroupTermsFunction().evaluate(vf, vf.createLiteral("foo"), vf.createLiteral("bar"));
		assertEquals("(foo bar)", group.stringValue());
	}
}
