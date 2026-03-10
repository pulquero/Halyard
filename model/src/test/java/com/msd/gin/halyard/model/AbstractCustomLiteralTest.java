package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

public abstract class AbstractCustomLiteralTest {
	protected final ValueFactory valueFactory = SimpleValueFactory.getInstance();

	protected abstract Literal createLiteral() throws Exception;
	protected abstract Literal createOtherLiteral() throws Exception;

	@Test
	public void testEqualsHashCode() throws Exception {
		Literal actual = createLiteral();
		Literal expected = valueFactory.createLiteral(actual.getLabel(), actual.getDatatype(), actual.getCoreDatatype());
		assertEquals(expected.hashCode(), actual.hashCode());
		assertTrue(expected.equals(actual));
	}

	@Test
	public void testNotEqual() throws Exception {
		Literal actual = createLiteral();
		Literal unexpected = createOtherLiteral();
		assertNotEquals(unexpected, actual);
	}

	@Test
	public void testNotEqual_empty() throws Exception {
		Literal actual = createLiteral();
		Literal unexpected = valueFactory.createLiteral("", actual.getDatatype(), actual.getCoreDatatype());
		assertNotEquals(unexpected, actual);
	}

	@Test
	public void testStringValue() throws Exception {
		Literal actual = createLiteral();
		Literal expected = valueFactory.createLiteral(actual.getLabel(), actual.getDatatype(), actual.getCoreDatatype());
		assertEquals(expected.stringValue(), actual.stringValue());
	}

	@Test
	public void testToString() throws Exception {
		Literal actual = createLiteral();
		Literal expected = valueFactory.createLiteral(actual.getLabel(), actual.getDatatype(), actual.getCoreDatatype());
		assertEquals(expected.toString(), actual.toString());
	}
}
