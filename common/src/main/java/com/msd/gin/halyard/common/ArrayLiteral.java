package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.AbstractDataLiteral;
import com.msd.gin.halyard.model.ObjectLiteral;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Arrays;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.json.JSONArray;

public final class ArrayLiteral extends AbstractDataLiteral implements ObjectLiteral<Object[]> {
	private static final long serialVersionUID = -6399155325720068478L;

	private final Object[] values;

	public ArrayLiteral(String s) {
		JSONArray arr = new JSONArray(s);
		int len = arr.length();
		this.values = new Object[len];
		for (int i=0; i<len; i++) {
			this.values[i] = arr.get(i);
		}
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
