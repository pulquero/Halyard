package com.msd.gin.halyard.spin.function.apf;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;

import com.msd.gin.halyard.model.vocabulary.APF;

public class SplitIRI implements TupleFunction {

	@Override
	public String getURI() {
		return APF.SPLIT_IRI.toString();
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>> evaluate(ValueFactory valueFactory, Value... args) throws QueryEvaluationException {
		if (args.length != 1 || !args[0].isIRI()) {
			throw new ValueExprEvaluationException(String.format("%s requires an IRI", getURI()));
		}
		IRI iri = (IRI) args[0];
		return new SingletonIteration<List<? extends Value>>(
				Arrays.asList(valueFactory.createLiteral(iri.getNamespace()), valueFactory.createLiteral(iri.getLocalName())));
	}
}
