package com.msd.gin.halyard.query.algebra.evaluation.function;

import com.msd.gin.halyard.model.TupleLiteral;
import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.function.Tuple;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TupleTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v1 = vf.createLiteral("foobar");
		Value v2 = vf.createIRI("http://foobar.org/");
		TupleLiteral l = (TupleLiteral) new Tuple().evaluate(ts, v1, v2);
		assertEquals(v1, l.objectValue()[0]);
		assertEquals(v2, l.objectValue()[1]);
	}
}
