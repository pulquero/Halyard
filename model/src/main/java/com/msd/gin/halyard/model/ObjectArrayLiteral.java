package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;
import org.json.JSONArray;

public final class ObjectArrayLiteral extends AbstractArrayLiteral<Object[]> {
	private static final long serialVersionUID = 6409948385114916596L;

	private final Object[] values;

	public ObjectArrayLiteral(String s) {
		this.values = parse(s);
	}

	public ObjectArrayLiteral(Object... values) {
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
	public Object[] objectValue() {
		return values;
	}

	@Override
	public Object[] elements() {
		return values;
	}

	@Override
	public int length() {
		return values.length;
	}

	public static Object[] objectArray(Literal l) {
		if (l instanceof AbstractArrayLiteral) {
			return ((AbstractArrayLiteral<?>)l).elements();
		} else {
			return parse(l.getLabel());
		}
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
}
