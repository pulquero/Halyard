package com.msd.gin.halyard.sail.search.function;

import com.msd.gin.halyard.model.ObjectArrayLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

import java.util.Locale;
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
	private static final Pattern OPERATORS = Pattern.compile("(^|\\s)((AND)|(OR)|(NOT)|(TO))($|\\s)");
	private static final Pattern RESERVED_CHARACTERS = Pattern.compile("[\\<\\>\\+\\-\\=\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\\\/]|(\\&\\&)|(\\|\\|)|" + OPERATORS.pattern());

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
			throw new QueryEvaluationException(String.format("Non-literal value: %s", args[0]));
		}
		Literal l = (Literal) args[0];
		if (HALYARD.ARRAY_TYPE.equals(l.getDatatype())) {
			Object[] entries = ObjectArrayLiteral.objectArray(l);
			Object[] escaped = new Object[entries.length];
			for (int i = 0; i < entries.length; i++) {
				Object o = entries[i];
				if (o instanceof String) {
					o = escape((String) o);
				}
				escaped[i] = o;
			}
			return new ObjectArrayLiteral(escaped);
		} else {
			String s = l.getLabel();
			return valueFactory.createLiteral(escape(s));
		}
	}

	private static String escape(String s) {
		StringBuilder buf = new StringBuilder(s.length());
		int end = 0;
		Matcher matcher = RESERVED_CHARACTERS.matcher(s);
		while (matcher.find()) {
			int start = matcher.start();
			buf.append(s.substring(end, start));
			end = matcher.end();
			String reserved = s.substring(start, end);
			if (OPERATORS.matcher(reserved).find()) {
				buf.append(reserved.toLowerCase(Locale.ROOT));
			} else if (!"<".equals(reserved) && !">".equals(reserved)) {
				buf.append("\\");
				buf.append(reserved);
			}
		}
		buf.append(s.substring(end));
		return buf.toString();
	}
}
