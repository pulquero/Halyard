package com.msd.gin.halyard.sail.search.function;

import com.google.common.collect.Sets;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public class EscapeTerm implements Function {
	private static final Pattern RESERVED_CHARACTERS = Pattern.compile("[\\<\\>\\+\\-\\=\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\\\/]|(\\&\\&)|(\\|\\|)|(AND)|(OR)|(NOT)|(TO)");
	private static final Set<String> OPERATORS = Sets.newHashSet("AND", "OR", "NOT", "TO");

	@Override
	public String getURI() {
		return HALYARD.ESCAPE_TERM_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 1) {
			throw new QueryEvaluationException("Missing arguments");
		}

		if (!args[0].isLiteral()) {
			throw new QueryEvaluationException("Invalid value");
		}
		String s = ((Literal) args[0]).stringValue();
		StringBuilder buf = new StringBuilder(s.length());
		int end = 0;
		Matcher matcher = RESERVED_CHARACTERS.matcher(s);
		while (matcher.find()) {
			int start = matcher.start();
			buf.append(s.substring(end, start));
			end = matcher.end();
			String reserved = s.substring(start, end);
			if (OPERATORS.contains(reserved)) {
				buf.append(reserved.toLowerCase(Locale.ROOT));
			} else if (!"<".equals(reserved) && !">".equals(reserved)) {
				buf.append("\\");
				buf.append(reserved);
			}
		}
		buf.append(s.substring(end));
		return valueFactory.createLiteral(buf.toString());
	}

}
