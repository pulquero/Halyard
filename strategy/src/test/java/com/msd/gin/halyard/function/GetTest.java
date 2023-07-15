package com.msd.gin.halyard.function;

import com.msd.gin.halyard.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.common.TupleLiteral;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v1 = new TupleLiteral(vf.createLiteral("foo"), vf.createLiteral("bar"));
		Value v2 = vf.createLiteral(0);
		Literal l = (Literal) new Get().evaluate(ts, v1, v2);
		assertEquals(vf.createLiteral("foo"), l);
	}
}
