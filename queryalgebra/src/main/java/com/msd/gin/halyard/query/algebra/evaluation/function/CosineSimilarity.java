package com.msd.gin.halyard.query.algebra.evaluation.function;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.model.ArrayLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

@MetaInfServices(Function.class)
public final class CosineSimilarity implements Function {

	@Override
	public String getURI() {
		return HALYARD.COSINE_SIMILARITY_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 2 || !args[0].isLiteral() || !args[1].isLiteral()) {
			throw new ValueExprEvaluationException(String.format("%s requires 2 literal arguments", getURI()));
		}
		Object[] v1 = ArrayLiteral.objectArray((Literal) args[0]);
		Object[] v2 = ArrayLiteral.objectArray((Literal) args[1]);
		if (v1.length != v2.length) {
			throw new ValueExprEvaluationException("Arrays have incompatible dimensions");
		}
		double dot = 0.0;
		double normSqr1 = 0.0;
		double normSqr2 = 0.0;
		for (int i=0; i<v1.length; i++) {
			double x = ((Number)v1[i]).doubleValue();
			double y = ((Number)v2[i]).doubleValue();
			dot += x*y;
			normSqr1 += x*x;
			normSqr2 += y*y;
		}
		return valueFactory.createLiteral(dot/Math.sqrt(normSqr1*normSqr2));
	}

}
