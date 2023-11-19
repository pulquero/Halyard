package com.msd.gin.halyard.sail.search.function;

import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public class SearchField implements Function {
	@Override
	public String getURI() {
		return HALYARD.SEARCH_FIELD_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 2) {
			throw new QueryEvaluationException("Missing arguments");
		}

		if (!args[0].isLiteral()) {
			throw new QueryEvaluationException("Invalid field name");
		}
		if (!args[1].isLiteral()) {
			throw new QueryEvaluationException("Invalid term");
		}

		return valueFactory.createLiteral(args[0].stringValue() + ":" + args[1].stringValue());
	}

}
