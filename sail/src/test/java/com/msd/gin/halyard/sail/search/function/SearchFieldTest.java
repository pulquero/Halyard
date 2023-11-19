package com.msd.gin.halyard.sail.search.function;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SearchFieldTest {
	private final ValueFactory vf = SimpleValueFactory.getInstance();

	@Test
	public void test() {
		Literal escaped = (Literal) new SearchField().evaluate(vf, vf.createLiteral("foo"), vf.createLiteral("bar"));
		assertEquals("foo:bar", escaped.stringValue());
	}
}
