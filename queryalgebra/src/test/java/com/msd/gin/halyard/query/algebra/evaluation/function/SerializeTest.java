package com.msd.gin.halyard.query.algebra.evaluation.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;

public class SerializeTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v = vf.createTriple(vf.createIRI("http://whatever.com/subj"), vf.createIRI("http://whatever.com/pred"), vf.createLiteral("foobar"));
		Literal l = (Literal) new Serialize().evaluate(ts, v);
		assertEquals(vf.createLiteral("<<<http://whatever.com/subj> <http://whatever.com/pred> \"foobar\">>"), l);
	}
}
