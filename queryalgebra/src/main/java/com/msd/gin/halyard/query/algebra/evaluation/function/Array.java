package com.msd.gin.halyard.query.algebra.evaluation.function;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.model.ArrayLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

/**
 * Constructor function for array literals.
 */
@MetaInfServices(Function.class)
public final class Array implements Function {

	@Override
	public String getURI() {
		return HALYARD.ARRAY_TYPE.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		return ArrayLiteral.createFromValues(args);
	}
}
