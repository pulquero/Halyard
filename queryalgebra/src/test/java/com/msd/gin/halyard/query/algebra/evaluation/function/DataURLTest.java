package com.msd.gin.halyard.query.algebra.evaluation.function;

import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.function.DataURL;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataURLTest {
	@Test
	public void testString() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v = vf.createLiteral("foo bar");
		IRI dataUrl = (IRI) new DataURL().evaluate(ts, v);
		assertEquals("data:,foo%20bar", dataUrl.stringValue());
	}

	@Test
	public void testNTriplesString() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v = vf.createLiteral("foo bar");
		Value mime = vf.createLiteral("application/n-triples");
		IRI dataUrl = (IRI) new DataURL().evaluate(ts, v, mime);
		assertEquals("data:application/n-triples,%22foo%20bar%22", dataUrl.stringValue());
	}

	@Test
	public void testNTriplesDatatype() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v = vf.createLiteral(101);
		Value mime = vf.createLiteral("application/n-triples");
		IRI dataUrl = (IRI) new DataURL().evaluate(ts, v, mime);
		assertEquals("data:application/n-triples,%22101%22%5E%5E%3Chttp:%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23int%3E", dataUrl.stringValue());
	}
}
