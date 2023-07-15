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
public final class Slice implements Function {

	@Override
	public String getURI() {
		return HALYARD.SLICE_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 3 || !TupleLiteral.isTupleLiteral(args[0]) || !args[1].isLiteral() || !args[2].isLiteral()) {
			throw new ValueExprEvaluationException(String.format("%s requires a tuple, a start index and a length", getURI()));
		}
		Value[] arr = TupleLiteral.arrayValue((Literal) args[0], valueFactory);
		int startIndex = ((Literal) args[1]).intValue();
		int len = ((Literal) args[2]).intValue();
		if (startIndex < 0 || startIndex > arr.length) {
			throw new ValueExprEvaluationException(String.format("Start index out of bounds", getURI()));
		}
		if (startIndex + len > arr.length) {
			throw new ValueExprEvaluationException(String.format("Length too long", getURI()));
		}
		Value[] sliceArr = new Value[len];
		System.arraycopy(arr, startIndex, sliceArr, 0, len);
		return new TupleLiteral(sliceArr);
	}

}
