package com.msd.gin.halyard.function;

import com.msd.gin.halyard.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.common.TupleLiteral;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SliceTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v1 = vf.createLiteral("foobar");
		Value v2 = vf.createIRI("http://foobar.org/");
		Value v3 = vf.createLiteral(3);
		TupleLiteral tl = new TupleLiteral(v1, v2, v3);
		TupleLiteral slice = (TupleLiteral) new Slice().evaluate(ts, tl, vf.createLiteral(1), vf.createLiteral(2));
		assertEquals(new TupleLiteral(v2, v3), slice);
	}
}
