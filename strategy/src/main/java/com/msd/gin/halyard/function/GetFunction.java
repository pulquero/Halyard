package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.TupleLiteral;
import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public final class GetFunction implements Function {

	@Override
	public String getURI() {
		return HALYARD.GET_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 2 || !(args[0] instanceof TupleLiteral) || !(args[1] instanceof Literal)) {
			throw new ValueExprEvaluationException(String.format("%s requires a tuple and an index", getURI()));
		}
		TupleLiteral tl = (TupleLiteral) args[0];
		Literal idx = (Literal) args[1];
		return tl.objectValue()[idx.intValue()];
	}

}
