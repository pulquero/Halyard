package com.msd.gin.halyard.query.algebra.evaluation.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.model.FloatArrayLiteral;
import com.msd.gin.halyard.model.ObjectArrayLiteral;
import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;

public class CosineSimilarityTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		Value v1 = new FloatArrayLiteral(1.0f, 0.0f);
		Value v2 = new FloatArrayLiteral(1.0f, 1.0f);
		Literal l = (Literal) new CosineSimilarity().evaluate(ts, v1, v2);
		assertEquals(1.0/Math.sqrt(2), l.doubleValue());
	}

	@Test
	public void test_objectArray() {
		TripleSource ts = new EmptyTripleSource();
		Value v1 = new ObjectArrayLiteral(1.0f, 1.0f);
		Value v2 = new ObjectArrayLiteral(1.0f, 0.0f);
		Literal l = (Literal) new CosineSimilarity().evaluate(ts, v1, v2);
		assertEquals(1.0/Math.sqrt(2), l.doubleValue());
	}
}
