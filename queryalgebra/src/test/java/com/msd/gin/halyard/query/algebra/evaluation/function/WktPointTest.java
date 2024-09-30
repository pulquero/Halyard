package com.msd.gin.halyard.query.algebra.evaluation.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype.GEO;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;

public class WktPointTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		assertEquals(vf.createLiteral("POINT (3.4 2)", GEO.WKT_LITERAL), new WktPoint().evaluate(ts, vf.createLiteral(3.4), vf.createLiteral(2)));
	}
}
