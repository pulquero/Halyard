package com.msd.gin.halyard.spin.function.halyard;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

import com.msd.gin.halyard.model.TupleLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.evaluation.function.ExtendedTupleFunction;
import com.msd.gin.halyard.spin.function.AbstractSpinFunction;
import com.msd.gin.halyard.spin.function.InverseMagicProperty;

public class FromTuple extends AbstractSpinFunction implements ExtendedTupleFunction, InverseMagicProperty {
	public FromTuple() {
		super(HALYARD.FROM_TUPLE.stringValue());
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>> evaluate(
			TripleSource tripleSource, Value... args) throws QueryEvaluationException {
		if (args.length != 1 || !TupleLiteral.isTupleLiteral(args[0])) {
			throw new ValueExprEvaluationException(String.format("%s requires a tuple", getURI()));
		}
		Value[] elements = TupleLiteral.valueArray((Literal)args[0], tripleSource.getValueFactory());
		return new SingletonIteration<>(Arrays.asList(elements));
	}
}
