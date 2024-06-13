package com.msd.gin.halyard.query.algebra.evaluation.function;

import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;

public interface ExtendedTupleFunction extends TupleFunction {
	CloseableIteration<? extends List<? extends Value>> evaluate(
			TripleSource tripleSource, Value... args) throws QueryEvaluationException;

	@Override
	default CloseableIteration<? extends List<? extends Value>> evaluate(ValueFactory vf,
			Value... args) throws QueryEvaluationException {
		throw new UnsupportedOperationException();
	}
}
