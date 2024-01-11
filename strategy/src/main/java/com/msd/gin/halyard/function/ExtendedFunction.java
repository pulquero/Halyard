package com.msd.gin.halyard.function;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;

public interface ExtendedFunction extends Function {
	Value evaluate(TripleSource tripleSource, QueryEvaluationContext ctx, Value... args) throws ValueExprEvaluationException;

	@Override
	default Value evaluate(TripleSource tripleSource, Value... args) throws ValueExprEvaluationException {
		throw new UnsupportedOperationException();
	}

	@Override
	default Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		throw new UnsupportedOperationException();
	}
}
