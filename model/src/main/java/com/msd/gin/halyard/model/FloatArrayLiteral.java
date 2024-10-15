package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;
import org.json.JSONArray;

import com.msd.gin.halyard.model.vocabulary.HALYARD;

public final class FloatArrayLiteral extends AbstractArrayLiteral<float[]> {
	private static final long serialVersionUID = -5786623818445151749L;

	private final float[] values;

	public FloatArrayLiteral(float... values) {
		this.values = values;
	}

	@Override
	public String getLabel() {
		JSONArray arr = new JSONArray();
		for (float o : this.values) {
			arr.put(o);
		}
		return arr.toString(0);
	}

	@Override
	public float[] objectValue() {
		return values;
	}

	@Override
	public Object[] elements() {
		Object[] arr = new Object[values.length];
		for (int i=0; i<values.length; i++) {
			arr[i] = values[i];
		}
		return arr;
	}

	@Override
	public int length() {
		return values.length;
	}

	public static float[] floatArray(Literal l) {
		if (l instanceof FloatArrayLiteral) {
			return ((FloatArrayLiteral)l).objectValue();
		} else if (HALYARD.ARRAY_TYPE.equals(l.getDatatype())){
			Object[] arr = ObjectArrayLiteral.objectArray(l);
			float[] farr = new float[arr.length];
			for (int i=0; i<arr.length; i++) {
				farr[i] = (Float) arr[i];
			}
			return farr;
		} else {
			throw new IllegalArgumentException("Incorrect datatype");
		}
	}
}
