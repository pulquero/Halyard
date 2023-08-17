package com.msd.gin.halyard.sail.search.function;

import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public class PhraseTerms implements Function {
	@Override
	public String getURI() {
		return HALYARD.PHRASE_TERMS_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length < 1) {
			throw new QueryEvaluationException("Missing arguments");
		}

		int end = args.length;
		String slop;
		Value lastArg = args[end - 1];
		if (!lastArg.isLiteral()) {
			throw new QueryEvaluationException("Invalid value");
		}
		String lastArgLabel = lastArg.stringValue();
		if (lastArgLabel.startsWith("~")) {
			slop = lastArgLabel;
			end--;
		} else {
			slop = "";
		}
		StringBuilder buf = new StringBuilder();
		buf.append("\"");
		String sep = "";
		for (int i = 0; i < end; i++) {
			Value arg = args[i];
			if (!arg.isLiteral()) {
				throw new QueryEvaluationException("Invalid value");
			}
			buf.append(sep);
			buf.append(arg.stringValue());
			sep = " ";
		}
		buf.append("\"");
		buf.append(slop);
		return valueFactory.createLiteral(buf.toString());
	}

}
