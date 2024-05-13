package com.msd.gin.halyard.query.algebra.evaluation.function;

import com.msd.gin.halyard.model.TupleLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public final class Get implements Function {

	@Override
	public String getURI() {
		return HALYARD.GET_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 2 || !TupleLiteral.isTupleLiteral(args[0]) || !args[1].isLiteral()) {
			throw new ValueExprEvaluationException(String.format("%s requires a tuple and a 1-based index", getURI()));
		}
		Value[] elements = TupleLiteral.valueArray((Literal)args[0], valueFactory);
		Literal idxArg = (Literal) args[1];
		int idx0 = idxArg.intValue() - 1;
		return elements[idx0];
	}

}
