package com.msd.gin.halyard.sail.search;

import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public class PhraseTermsFunction implements Function {
	@Override
	public String getURI() {
		return HALYARD.PHRASE_TERMS_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length < 1) {
			throw new QueryEvaluationException("Missing arguments");
		}

		StringBuilder buf = new StringBuilder();
		buf.append("\"");
		String sep = "";
		for (Value arg : args) {
			if (!arg.isLiteral()) {
				throw new QueryEvaluationException("Invalid value");
			}
			buf.append(sep);
			buf.append(arg.stringValue());
			sep = " ";
		}
		buf.append("\"");
		return valueFactory.createLiteral(buf.toString());
	}

}
