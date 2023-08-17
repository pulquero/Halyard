package com.msd.gin.halyard.sail.search.function;

import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public class GroupTerms implements Function {
	@Override
	public String getURI() {
		return HALYARD.GROUP_TERMS_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length < 1) {
			throw new QueryEvaluationException("Missing arguments");
		}

		int start = 0;
		String operator;
		Value firstArg = args[0];
		if (!firstArg.isLiteral()) {
			throw new QueryEvaluationException("Invalid value");
		}
		String firstArgLabel = firstArg.stringValue();
		if (firstArgLabel.equals("OR") || firstArgLabel.equals("||") || firstArgLabel.equals("AND") || firstArgLabel.equals("&&")) {
			operator = " " + firstArgLabel + " ";
			start++;
		} else {
			operator = " ";
		}

		StringBuilder buf = new StringBuilder();
		buf.append("(");
		String sep = "";
		for (int i = start; i < args.length; i++) {
			Value arg = args[i];
			if (!arg.isLiteral()) {
				throw new QueryEvaluationException("Invalid value");
			}
			buf.append(sep);
			buf.append(arg.stringValue());
			sep = operator;
		}
		buf.append(")");
		return valueFactory.createLiteral(buf.toString());
	}

}
