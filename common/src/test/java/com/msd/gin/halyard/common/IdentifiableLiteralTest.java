package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.LiteralTest;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class IdentifiableLiteralTest extends LiteralTest {

	@Override
	protected IRI datatype(String dt) {
		return new IdentifiableIRI(dt);
	}

	@Override
	protected Literal literal(String label) {
		return new IdentifiableLiteral(label);
	}

	@Override
	protected Literal literal(String label, String lang) {
		return new IdentifiableLiteral(label, lang);
	}

	@Override
	protected Literal literal(String label, IRI dt) {
		return new IdentifiableLiteral(label, dt);
	}

	@Override
	protected Literal literal(String label, CoreDatatype dt) {
		return new IdentifiableLiteral(label, dt);
	}

	@Test
	public void testHashCodeForNonCanonicalDate() {
		Literal expected = SimpleValueFactory.getInstance().createLiteral("2022-03-29Z", XSD.DATE);
		Literal actual = new IdentifiableLiteral("2022-03-29+00:00", XSD.DATE);
		assertEquals(expected.hashCode(), actual.hashCode());
	}
}
