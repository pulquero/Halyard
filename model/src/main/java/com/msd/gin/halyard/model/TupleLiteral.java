package com.msd.gin.halyard.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;

import com.msd.gin.halyard.model.vocabulary.HALYARD;

public final class TupleLiteral extends AbstractDataLiteral implements ObjectLiteral<Value[]> {
	private static final long serialVersionUID = 1465080710600525119L;

	public static boolean isTupleLiteral(Value v) {
		return v != null && v.isLiteral() && HALYARD.TUPLE_TYPE.equals(((Literal)v).getDatatype());
	}

	public static TupleLiteral asTupleLiteral(Value v) {
		return (v instanceof TupleLiteral) ? (TupleLiteral) v : new TupleLiteral(((Literal)v).getLabel());
	}

	public static Value[] valueArray(Literal l, ValueFactory vf) {
		if (l instanceof TupleLiteral) {
			return ((TupleLiteral)l).values;
		} else {
			return parse(l.getLabel(), vf);
		}
	}

	private static Value[] parse(CharSequence s) {
		return parse(s, SimpleValueFactory.getInstance());
	}

	private static Value[] parse(CharSequence s, ValueFactory vf) {
		List<Value> values = new ArrayList<>();
		int len = s.length();
		for (int i=0; i<len; i++) {
			char ch = s.charAt(i);
			if (ch == '<') {
				i++;
				int start = i;
				while (i < len && (ch = s.charAt(i)) != '>') {
					if (ch == '\\') {
						i++;
					}
					i++;
				}
				IRI v = vf.createIRI(s.subSequence(start, i).toString());
				values.add(v);
			} else if (ch == '"') {
				i++;
				int start = i;
				while (i < len && (ch = s.charAt(i)) != '"') {
					if (ch == '\\') {
						i++;
					}
					i++;
				}
				String label = NTriplesUtil.unescapeString(s.subSequence(start, i).toString());
				i++;  // end quote
				ch = i < len ? s.charAt(i) : ' ';  // peak ahead
				Literal v;
				if (ch == '^') {
					i += 3;
					start = i;
					while (i < len && (ch = s.charAt(i)) != '>') {
						if (ch == '\\') {
							i++;
						}
						i++;
					}
					IRI dt = vf.createIRI(s.subSequence(start, i).toString());
					v = vf.createLiteral(label, dt);
				} else if (ch == '@') {
					start = i + 1;
					while (i < len && (ch = s.charAt(i)) != ' ') {
						i++;
					}
					v = vf.createLiteral(label, s.subSequence(start, i).toString());
				} else {
					v = vf.createLiteral(label);
				}
				values.add(v);
			} else if (ch == ' ') {
			} else {
				throw new RDFParseException(String.format("Invalid tuple: %s", s));
			}
		}
		return values.toArray(new Value[values.size()]);
	}

	private final Value[] values;

	public TupleLiteral(String s) {
		this.values = parse(s);
	}

	public TupleLiteral(Value... values) {
		this.values = values;
	}

	@Override
	public String getLabel() {
		StringBuilder buf = new StringBuilder();
		String sep = "";
		for (Value v : values) {
			buf.append(sep);
			try {
				NTriplesUtil.append(v, buf);
			} catch (IOException e) {
				throw new AssertionError(e);
			}
			sep = " ";
		}
		return buf.toString();
	}

	@Override
	public IRI getDatatype() {
		return HALYARD.TUPLE_TYPE;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.NONE;
	}

	@Override
	public Value[] objectValue() {
		return values;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof Literal) {
			Literal other = (Literal) o;
			return HALYARD.TUPLE_TYPE.equals(other.getDatatype()) && Arrays.equals(values, valueArray(other, SimpleValueFactory.getInstance()));
		} else {
			return super.equals(o);
		}
	}
}
