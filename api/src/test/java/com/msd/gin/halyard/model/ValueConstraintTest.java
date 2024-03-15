package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

public class ValueConstraintTest {

	@Test
	public void testIRIs() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ValueConstraint vc = new ValueConstraint(ValueType.IRI);
		assertFalse(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createIRI("urn:foo:bar")));
	}

	@Test
	public void testBNodes() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ValueConstraint vc = new ValueConstraint(ValueType.BNODE);
		assertTrue(vc.test(vf.createBNode()));
		assertFalse(vc.test(vf.createIRI("urn:foo:bar")));
	}

	@Test
	public void testAllLiterals() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ValueConstraint vc = new ValueConstraint(ValueType.LITERAL);
		assertTrue(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createLiteral("foobar")));
		assertTrue(vc.test(vf.createLiteral("foo", "en")));
	}
}
