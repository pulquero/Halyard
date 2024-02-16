package com.msd.gin.halyard.query.algebra.evaluation.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.math.BigInteger;
import java.util.Collections;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.common.ArrayLiteral;
import com.msd.gin.halyard.common.MapLiteral;
import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;

public class DynamicFunctionRegistryTest {

	@Test
	public void testNoSuchFunction() {
		boolean result = new DynamicFunctionRegistry().has("foo#bar");
		assertFalse(result);
	}

	@Test
	public void testDaysFromDuration() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value result = new DynamicFunctionRegistry().get("http://www.w3.org/2005/xpath-functions#days-from-duration").get().evaluate(ts,
				vf.createLiteral("P3DT10H", XSD.DAYTIMEDURATION));
		assertEquals(3, ((Literal) result).intValue());
	}

	@Test
	public void testSin() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value result = new DynamicFunctionRegistry().get("http://www.w3.org/2005/xpath-functions/math#sin").get().evaluate(ts, vf.createLiteral(Math.PI / 2.0));
		assertEquals(1.0, ((Literal) result).doubleValue(), 0.001);
	}

	@Test
	public void testIsWholeNumber() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		Value result = new DynamicFunctionRegistry().get("http://saxon.sf.net/#is-whole-number").get().evaluate(ts, vf.createLiteral(3));
		assertEquals(true, ((Literal) result).booleanValue());
	}

	@Test
	public void testArray() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		ArrayLiteral result = (ArrayLiteral) new DynamicFunctionRegistry().get("http://www.w3.org/2005/xpath-functions/array#put").get().evaluate(ts,
				new ArrayLiteral("foo", "bar"), vf.createLiteral(2), vf.createLiteral(5));
		assertEquals(2, result.objectValue().length);
		// NB: xsd:ints get coerced to xsd:integers
		assertEquals(BigInteger.valueOf(5), result.objectValue()[1]);
	}

	@Test
	public void testMap() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		MapLiteral result = (MapLiteral) new DynamicFunctionRegistry().get("http://www.w3.org/2005/xpath-functions/map#put").get().evaluate(ts,
				new MapLiteral(Collections.singletonMap("foo", "bar")),
				vf.createLiteral("key"),
				vf.createLiteral(5));
		assertEquals(2, result.objectValue().size());
		// NB: xsd:ints get coerced to xsd:integers
		assertEquals(BigInteger.valueOf(5), result.objectValue().get("key"));
	}
}
