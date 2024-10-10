package com.msd.gin.halyard.query.algebra.evaluation.function;

import java.util.Locale;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.model.vocabulary.HALYARD;

@MetaInfServices(Function.class)
public class Like implements Function {
	@Override
	public String getURI() {
		return HALYARD.LIKE_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length < 2) {
			throw new ValueExprEvaluationException("At least two arguments required");
		}

		Value val = args[0];
        String strVal;
        if (val.isIRI()) {
            strVal = val.stringValue();
        } else if (val.isLiteral()) {
            strVal = ((Literal) val).getLabel();
        } else {
        	throw new ValueExprEvaluationException("Unsupported value");
        }
        String pattern = ((Literal) args[1]).getLabel();
        boolean isCaseSensitive = (args.length == 3 && BooleanLiteral.TRUE.equals(args[2]));
        if (isCaseSensitive) {
            // Convert strVal to lower case, just like the pattern has been done
            strVal = strVal.toLowerCase(Locale.ROOT);
            pattern = pattern.toLowerCase();
        }
        int valIndex = 0;
        int prevPatternIndex = -1;
        int patternIndex = pattern.indexOf('*');
        if (patternIndex == -1) {
            // No wildcards
            return valueFactory.createLiteral(pattern.equals(strVal));
        }
        String snippet;
        if (patternIndex > 0) {
            // Pattern does not start with a wildcard, first part must match
            snippet = pattern.substring(0, patternIndex);
            if (!strVal.startsWith(snippet)) {
                return BooleanLiteral.FALSE;
            }
            valIndex += snippet.length();
            prevPatternIndex = patternIndex;
            patternIndex = pattern.indexOf('*', patternIndex + 1);
        }
        while (patternIndex != -1) {
            // Get snippet between previous wildcard and this wildcard
            snippet = pattern.substring(prevPatternIndex + 1, patternIndex);
            // Search for the snippet in the value
            valIndex = strVal.indexOf(snippet, valIndex);
            if (valIndex == -1) {
                return BooleanLiteral.FALSE;
            }
            valIndex += snippet.length();
            prevPatternIndex = patternIndex;
            patternIndex = pattern.indexOf('*', patternIndex + 1);
        }
        // Part after last wildcard
        snippet = pattern.substring(prevPatternIndex + 1);
        if (snippet.length() > 0) {
            // Pattern does not end with a wildcard.
            // Search last occurence of the snippet.
            valIndex = strVal.indexOf(snippet, valIndex);
            int i;
            while ((i = strVal.indexOf(snippet, valIndex + 1)) != -1) {
                // A later occurence was found.
                valIndex = i;
            }
            if (valIndex == -1) {
                return BooleanLiteral.FALSE;
            }
            valIndex += snippet.length();
            if (valIndex < strVal.length()) {
                // Some characters were not matched
                return BooleanLiteral.FALSE;
            }
        }
        return BooleanLiteral.FALSE;
	}
}
