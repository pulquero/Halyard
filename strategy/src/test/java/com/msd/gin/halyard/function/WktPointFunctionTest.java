package com.msd.gin.halyard.function;

import com.msd.gin.halyard.algebra.evaluation.EmptyTripleSource;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype.GEO;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WktPointFunctionTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		assertEquals(vf.createLiteral("POINT (3.4 2)", GEO.WKT_LITERAL), new WktPointFunction().evaluate(ts, vf.createLiteral(3.4), vf.createLiteral(2)));
	}
}
