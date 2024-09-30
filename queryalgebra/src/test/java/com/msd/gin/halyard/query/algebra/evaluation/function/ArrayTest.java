package com.msd.gin.halyard.query.algebra.evaluation.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.model.ArrayLiteral;
import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;

public class ArrayTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v1 = vf.createLiteral("foobar");
		Value v2 = vf.createLiteral(5);
		ArrayLiteral l = (ArrayLiteral) new Array().evaluate(ts, v1, v2);
		assertEquals("foobar", l.objectValue()[0]);
		assertEquals(5, l.objectValue()[1]);
	}

	@Test
	public void test_invalid() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v1 = vf.createLiteral("foobar");
		Value v2 = vf.createIRI("http://foobar.org/");
		assertThrows(ValueExprEvaluationException.class, () -> new Array().evaluate(ts, v1, v2));
	}
}
