package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;

public final class TupleLiteral extends AbstractDataLiteral implements ObjectLiteral<Value[]> {
	private static final long serialVersionUID = 1465080710600525119L;

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
				i++;
				ch = i < len ? s.charAt(i++) : ' ';
				Literal v;
				if (ch == '^') {
					i += 2;
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
					start = i;
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
				throw new RDFParseException("Invalid tuple");
			}
		}
		return values.toArray(new Value[values.size()]);
	}

	private final Value[] values;

	public TupleLiteral(String s) {
		this.values = parse(s, SimpleValueFactory.getInstance());
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

		if (o instanceof TupleLiteral) {
			TupleLiteral other = (TupleLiteral) o;
			return Arrays.equals(values, other.values);
		} else if (o instanceof Literal) {
			Literal other = (Literal) o;

			// Compare labels
			if (!getLabel().equals(other.getLabel())) {
				return false;
			}

			// Compare datatypes
			if (!getDatatype().equals(other.getDatatype())) {
				return false;
			}
			return true;
		}
		return false;
	}
}
