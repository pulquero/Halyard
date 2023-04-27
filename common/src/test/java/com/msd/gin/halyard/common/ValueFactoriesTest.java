package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValueFactoriesTest {
	@Test
	public void testConvertValues() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("iri", vf.createIRI("http://foobar"));
		bs.addBinding("bnode", vf.createBNode("abc"));
		bs.addBinding("stringLiteral", vf.createLiteral("abc"));
		bs.addBinding("langLiteral", vf.createLiteral("abc", "en"));
		bs.addBinding("datatypeLiteral", vf.createLiteral(7));
		bs.addBinding("triple", vf.createTriple(vf.createIRI("http://foobar"), vf.createIRI("http://foobar"), vf.createIRI("http://foobar")));
		BindingSet out = ValueFactories.convertValues(bs, new IdValueFactory(null));
		for (Binding b : out) {
			assertTrue(b.getValue() instanceof IdentifiableValue);
		}
	}
}
