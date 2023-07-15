package com.msd.gin.halyard.sail.search.function;

import com.msd.gin.halyard.sail.search.function.GroupTerms;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class GroupTermsTest {
	private final ValueFactory vf = SimpleValueFactory.getInstance();

	@Test
	public void test() {
		Literal group = (Literal) new GroupTerms().evaluate(vf, vf.createLiteral("foo"), vf.createLiteral("bar"));
		assertEquals("(foo bar)", group.stringValue());
	}
}
