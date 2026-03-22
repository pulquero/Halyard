package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;
import org.json.JSONStringer;

public final class DoubleArrayLiteral extends AbstractArrayLiteral<double[]> {
	private static final long serialVersionUID = -8105005334950424983L;

	private final double[] values;

	public DoubleArrayLiteral(double... values) {
		this.values = values;
	}

	@Override
	public String getLabel() {
		JSONStringer writer = new JSONStringer();
		writer.array();
		for (double o : this.values) {
			writer.value(o);
		}
		writer.endArray();
		return writer.toString();
	}

	@Override
	public double[] objectValue() {
		return values;
	}

	@Override
	public Class<?> componentType() {
		return Double.class;
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

	public static double[] doubleArray(Literal l) {
		if (l instanceof DoubleArrayLiteral) {
			return ((DoubleArrayLiteral)l).objectValue();
		} else if (AbstractArrayLiteral.isArrayLiteral(l)) {
			Object[] arr = ObjectArrayLiteral.objectArray(l);
			double[] darr = new double[arr.length];
			for (int i=0; i<arr.length; i++) {
				darr[i] = (Float) arr[i];
			}
			return darr;
		} else {
			throw new IllegalArgumentException("Incorrect datatype");
		}
	}
}
