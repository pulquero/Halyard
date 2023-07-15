package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.TupleLiteral;
import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

/**
 * Constructor function for tuple literals.
 */
@MetaInfServices(Function.class)
public final class Tuple implements Function {

	@Override
	public String getURI() {
		return HALYARD.TUPLE_TYPE.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		return new TupleLiteral(args);
	}

}
