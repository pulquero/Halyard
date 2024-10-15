package com.msd.gin.halyard.query.algebra.evaluation.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.model.FloatArrayLiteral;
import com.msd.gin.halyard.model.ObjectArrayLiteral;
import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;

public class ArrayTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v1 = vf.createLiteral("foobar");
		Value v2 = vf.createLiteral(5);
		Object[] objs = ObjectArrayLiteral.objectArray((Literal) new Array().evaluate(ts, v1, v2));
		assertEquals("foobar", objs[0]);
		assertEquals(5, objs[1]);
	}

	@Test
	public void test_float() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v1 = vf.createLiteral(2.5f);
		Value v2 = vf.createLiteral(5f);
		Object[] objs = ObjectArrayLiteral.objectArray((Literal) new Array().evaluate(ts, v1, v2));
		assertEquals(2.5f, objs[0]);
		assertEquals(5f, objs[1]);
		float[] farr = FloatArrayLiteral.floatArray((Literal) new Array().evaluate(ts, v1, v2));
		assertEquals(2.5f, farr[0]);
		assertEquals(5f, farr[1]);
	}

	@Test
	public void test_mixed() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value v1 = vf.createLiteral(-0.15f);
		Value v2 = vf.createLiteral("foobar");
		Object[] objs = ObjectArrayLiteral.objectArray((Literal) new Array().evaluate(ts, v1, v2));
		assertEquals(-0.15f, objs[0]);
		assertEquals("foobar", objs[1]);
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
