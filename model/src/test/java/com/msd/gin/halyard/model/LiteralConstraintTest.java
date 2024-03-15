package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.model.vocabulary.HALYARD;

public class LiteralConstraintTest {

	@Test
	public void testStringLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraint vc = new LiteralConstraint(XSD.STRING);
		assertFalse(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createLiteral("foobar")));
		assertFalse(vc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testLangLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraint vc = new LiteralConstraint("en");
		assertFalse(vc.test(vf.createLiteral(1)));
		assertFalse(vc.test(vf.createLiteral("foobar")));
		assertTrue(vc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testOtherLiteral() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraint vc = new LiteralConstraint(HALYARD.NON_STRING_TYPE);
		assertTrue(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createLiteral(new Date())));
		assertFalse(vc.test(vf.createLiteral("foobar")));
		assertFalse(vc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testAnyNumeric() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		LiteralConstraint vc = new LiteralConstraint(HALYARD.ANY_NUMERIC_TYPE);
		assertTrue(vc.test(vf.createLiteral(1)));
		assertTrue(vc.test(vf.createLiteral(1.8)));
		assertFalse(vc.test(vf.createLiteral("foobar")));
		assertFalse(vc.test(vf.createLiteral("foo", "en")));
	}

	@Test
	public void testEquals() {
		ValueConstraint vc = new ValueConstraint(ValueType.LITERAL);
		LiteralConstraint lc = new LiteralConstraint(XSD.STRING);
		assertNotEquals(vc, lc);
		assertNotEquals(lc, vc);
	}
}
