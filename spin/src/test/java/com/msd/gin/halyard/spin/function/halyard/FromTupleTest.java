package com.msd.gin.halyard.spin.function.halyard;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.junit.jupiter.api.Test;

import com.msd.gin.halyard.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.common.TupleLiteral;

public class FromTupleTest {
	@Test
	public void test() {
		TripleSource ts = new EmptyTripleSource();
		ValueFactory vf = ts.getValueFactory();
		TupleLiteral v = new TupleLiteral(vf.createLiteral("foo"), vf.createLiteral("bar"));
		try(CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new FromTuple().evaluate(ts, v)) {
			List<? extends Value> actual = iter.next();
			assertEquals(Arrays.asList(v.objectValue()), actual);
		}
	}
}
