package com.msd.gin.halyard.query.algebra.evaluation.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.model.ArrayLiteral;
import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;

public class CosineSimilarityTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		Value v1 = new ArrayLiteral(1, 0);
		Value v2 = new ArrayLiteral(1, 1);
		Literal l = (Literal) new CosineSimilarity().evaluate(ts, v1, v2);
		assertEquals(1.0/Math.sqrt(2), l.doubleValue());
	}
}
