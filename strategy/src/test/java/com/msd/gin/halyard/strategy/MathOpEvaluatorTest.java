package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.model.FloatArrayLiteral;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class MathOpEvaluatorTest {
	@Test
	public void testAdd_floatVector() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Literal r = new MathOpEvaluator().evaluate(new FloatArrayLiteral(0.7f, -1.2f), new FloatArrayLiteral(0.1f,0.5f), MathOp.PLUS, vf);
		assertArrayEquals(new float[] {0.8f, -0.70000005f}, FloatArrayLiteral.floatArray(r));
	}

	@Test
	public void testSubtract_floatVector() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Literal r = new MathOpEvaluator().evaluate(new FloatArrayLiteral(0.7f, -1.2f), new FloatArrayLiteral(0.1f,0.5f), MathOp.MINUS, vf);
		assertArrayEquals(new float[] {0.59999996f, -1.7f}, FloatArrayLiteral.floatArray(r));
	}

	@Test
	public void testScalarMultiply_floatVector() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Literal r = new MathOpEvaluator().evaluate(vf.createLiteral(0.7f), new FloatArrayLiteral(0.1f,0.5f), MathOp.MULTIPLY, vf);
		assertArrayEquals(new float[] {0.07f, 0.35f}, FloatArrayLiteral.floatArray(r));
	}

	@Test
	public void testScalarDivide_floatVector() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Literal r = new MathOpEvaluator().evaluate(new FloatArrayLiteral(-0.7f, 1.2f), vf.createLiteral(-0.15f), MathOp.DIVIDE, vf);
		assertArrayEquals(new float[] {4.6666665f, -8.0f}, FloatArrayLiteral.floatArray(r));
	}
}
