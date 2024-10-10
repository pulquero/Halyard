package com.msd.gin.halyard.query.algebra.evaluation.function;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.model.vocabulary.HALYARD;

@MetaInfServices(Function.class)
public final class Serialize implements Function {

	@Override
	public String getURI() {
		return HALYARD.SERIALIZE_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 1) {
			throw new ValueExprEvaluationException(String.format("%s requires 1 argument", getURI()));
		}
		return valueFactory.createLiteral(NTriplesUtil.toNTriplesString(args[0], true));
	}

}
