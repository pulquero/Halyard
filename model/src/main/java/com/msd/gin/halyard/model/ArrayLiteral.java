package com.msd.gin.halyard.model;

import java.util.Arrays;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.util.Values;
import org.json.JSONArray;

import com.msd.gin.halyard.model.vocabulary.HALYARD;

public final class ArrayLiteral extends AbstractDataLiteral implements ObjectLiteral<Object[]> {
	private static final long serialVersionUID = -6399155325720068478L;

	public static boolean isArrayLiteral(Value v) {
		return v != null && v.isLiteral() && HALYARD.ARRAY_TYPE.equals(((Literal)v).getDatatype());
	}

	public static Object[] objectArray(Literal l) {
		if (l instanceof ArrayLiteral) {
			return ((ArrayLiteral)l).values;
		} else {
			return parse(l.getLabel());
		}
	}

	public static Value[] toValues(Object[] oarr, ValueFactory vf) {
		Value[] varr = new Value[oarr.length];
		for (int i=0; i<oarr.length; i++) {
			varr[i] = Values.literal(vf, oarr[i], false);
		}
		return varr;
	}

	private static Object[] parse(CharSequence s) {
		JSONArray arr = new JSONArray(s.toString());
		int len = arr.length();
		Object[] values = new Object[len];
		for (int i=0; i<len; i++) {
			values[i] = arr.get(i);
		}
		return values;
	}

	private final Object[] values;

	public ArrayLiteral(String s) {
		this.values = parse(s);
	}

	public ArrayLiteral(Object... values) {
		this.values = values;
	}

	@Override
	public String getLabel() {
		JSONArray arr = new JSONArray();
		for (Object o : this.values) {
			arr.put(o);
		}
		return arr.toString(0);
	}

	@Override
	public IRI getDatatype() {
		return HALYARD.ARRAY_TYPE;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.NONE;
	}

	@Override
	public Object[] objectValue() {
		return values;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof ArrayLiteral) {
			ArrayLiteral other = (ArrayLiteral) o;
			return Arrays.equals(values, other.values);
		} else {
			return super.equals(o);
		}
	}
}
