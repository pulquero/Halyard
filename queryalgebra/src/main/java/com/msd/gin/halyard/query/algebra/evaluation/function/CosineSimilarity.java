package com.msd.gin.halyard.query.algebra.evaluation.function;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.model.AbstractArrayLiteral;
import com.msd.gin.halyard.model.FloatArrayLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

@MetaInfServices(Function.class)
public final class CosineSimilarity implements Function {

	@Override
	public String getURI() {
		return HALYARD.COSINE_SIMILARITY_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 2 || !AbstractArrayLiteral.isArrayLiteral(args[0]) || !AbstractArrayLiteral.isArrayLiteral(args[1])) {
			throw new ValueExprEvaluationException(String.format("%s requires 2 array arguments", getURI()));
		}
		float[] v1 = FloatArrayLiteral.floatArray((Literal) args[0]);
		float[] v2 = FloatArrayLiteral.floatArray((Literal) args[1]);
		if (v1.length != v2.length) {
			throw new ValueExprEvaluationException("Arrays have incompatible dimensions");
		}
		double dot = 0.0;
		double normSqr1 = 0.0;
		double normSqr2 = 0.0;
		for (int i=0; i<v1.length; i++) {
			double x = v1[i];
			double y = v2[i];
			dot += x*y;
			normSqr1 += x*x;
			normSqr2 += y*y;
		}
		return valueFactory.createLiteral(dot/Math.sqrt(normSqr1*normSqr2));
	}

}
