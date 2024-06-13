package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.common.TimestampedValueFactory;
import com.msd.gin.halyard.sail.HBaseTripleSource;

import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimestampTupleFunctionTest {
	private static final StatementIndices stmtIndices = StatementIndices.create();

	private TripleSource getStubTripleSource(long ts) {
		return new HBaseTripleSource(null, SimpleValueFactory.getInstance(), stmtIndices, 0, null) {
			@Override
			public TripleSource getTimestampedTripleSource() {
				return new TripleSource() {
					private final TimestampedValueFactory tsvf = new TimestampedValueFactory(stmtIndices.getRDFFactory());

					@Override
					public CloseableIteration<? extends Statement> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
						Statement stmt = getValueFactory().createStatement(subj, pred, obj);
						((Timestamped) stmt).setTimestamp(ts);
						return new SingletonIteration<Statement>(stmt);
					}

					@Override
					public ValueFactory getValueFactory() {
						return tsvf;
					}
				};
			}
		};
	}

	@Test
	public void testTimestampedStatements() {
		long ts = System.currentTimeMillis();
		TripleSource tripleSource = getStubTripleSource(ts);
		ValueFactory vf = tripleSource.getValueFactory();
		Resource subj = vf.createBNode();
		IRI pred = vf.createIRI(":prop");
		Value obj = vf.createBNode();
		CloseableIteration<? extends List<? extends Value>> iter = new TimestampTupleFunction().evaluate(tripleSource, subj, pred, obj);
		assertTrue(iter.hasNext());
		List<? extends Value> bindings = iter.next();
		assertEquals(1, bindings.size());
		assertEquals(ts, ((Literal) bindings.get(0)).calendarValue().toGregorianCalendar().getTimeInMillis());
		assertFalse(iter.hasNext());
		iter.close();
	}

	@Test
	public void testTripleTimestamp() {
		long ts = System.currentTimeMillis();
		TripleSource tripleSource = getStubTripleSource(ts);
		ValueFactory vf = tripleSource.getValueFactory();
		Resource subj = vf.createBNode();
		IRI pred = vf.createIRI(":prop");
		Value obj = vf.createBNode();
		CloseableIteration<? extends List<? extends Value>> iter = new TimestampTupleFunction().evaluate(tripleSource, vf.createTriple(subj, pred, obj));
		assertTrue(iter.hasNext());
		List<? extends Value> bindings = iter.next();
		assertEquals(1, bindings.size());
		assertEquals(ts, ((Literal) bindings.get(0)).calendarValue().toGregorianCalendar().getTimeInMillis());
		assertFalse(iter.hasNext());
		iter.close();
	}

	@Test(expected = ValueExprEvaluationException.class)
	public void testIncorrectNumberOfArgs() {
		long ts = System.currentTimeMillis();
		TripleSource tripleSource = getStubTripleSource(ts);
		ValueFactory vf = tripleSource.getValueFactory();
		new TimestampTupleFunction().evaluate(tripleSource, vf.createBNode());
	}

	@Test(expected = ValueExprEvaluationException.class)
	public void testIncorrectNumberArgType1() {
		long ts = System.currentTimeMillis();
		TripleSource tripleSource = getStubTripleSource(ts);
		ValueFactory vf = tripleSource.getValueFactory();
		new TimestampTupleFunction().evaluate(tripleSource, vf.createLiteral("foo"), null, null);
	}

	@Test(expected = ValueExprEvaluationException.class)
	public void testIncorrectNumberArgType2() {
		long ts = System.currentTimeMillis();
		TripleSource tripleSource = getStubTripleSource(ts);
		ValueFactory vf = tripleSource.getValueFactory();
		new TimestampTupleFunction().evaluate(tripleSource, vf.createBNode(), vf.createLiteral("foo"), null);
	}
}
