package com.msd.gin.halyard.model;

import java.util.Arrays;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.base.CoreDatatype.XSD;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.json.JSONArray;
import org.json.JSONException;

import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.model.vocabulary.HalyardDatatype;

public abstract class AbstractArrayLiteral<T> extends AbstractDataLiteral implements ObjectLiteral<T> {
	private static final long serialVersionUID = -6423024672894102212L;

	public static boolean isArrayLiteral(Value v) {
		return v != null && v.isLiteral() && isArrayLiteral((Literal)v);
	}

	public static boolean isArrayLiteral(Literal l) {
		return (l.getCoreDatatype() == HalyardDatatype.ARRAY) || HALYARD.ARRAY_TYPE.equals(l.getDatatype());
	}

	public static AbstractArrayLiteral<?> asArrayLiteral(Literal l) {
		if (l instanceof AbstractArrayLiteral<?>) {
			return (AbstractArrayLiteral<?>) l;
		} else {
			return create(l.getLabel());
		}
	}

	public static Value[] toValues(Object[] oarr, ValueFactory vf) {
		Value[] varr = new Value[oarr.length];
		for (int i=0; i<oarr.length; i++) {
			varr[i] = Values.literal(vf, oarr[i], false);
		}
		return varr;
	}

	public static AbstractArrayLiteral<?> create(String s) {
		Object[] values;
		Class<?> componentType;
		try {
			JSONArray jsonArr = new JSONArray(s);
			int len = jsonArr.length();
			values = new Object[len];
			if (len == 0) {
				componentType = Object.class;
			} else {
				Object value = jsonArr.get(0);
				values[0] = value;
				componentType = value.getClass();
				for (int i=1; i<len; i++) {
					value = jsonArr.get(i);
					values[i] = value;
					Class<?> nextType = value.getClass();
					if (nextType != componentType) {
						componentType = Object.class;
					}
				}
			}
		} catch (JSONException e) {
			throw new IllegalArgumentException(e);
		}

		if (componentType == Double.class) {
			double[] darr = new double[values.length];
			float[] farr = new float[values.length];
			componentType = Float.class;
			for (int i=0; i<values.length; i++) {
				double v = (Double) values[i];
				float x = (float) v;
				darr[i] = v;
				farr[i] = x;
				if (Math.abs(v - x) > Math.ulp(v)) {
					componentType = Double.class;
				}
			}
			if (componentType == Float.class) {
				return new FloatArrayLiteral(farr);
			} else {
				return new DoubleArrayLiteral(darr);
			}
		} else {
			return new ObjectArrayLiteral(values, componentType);
		}
	}

	public static AbstractArrayLiteral<?> createFromValues(Value[] values) {
		AbstractArrayLiteral<?> arrLiteral = null;
		if (values.length > 0) {
			Literal l = asLiteral(values[0]);
			if (l.getCoreDatatype().asXSDDatatypeOrNull() == XSD.DOUBLE) {
				double[] darr = new double[values.length];
				darr[0] = l.doubleValue();
				for (int i=1; i<values.length; i++) {
					l = asLiteral(values[i]);
					if (l.getCoreDatatype().asXSDDatatypeOrNull() != XSD.DOUBLE) {
						darr = null;
						break;
					}
					darr[i] = l.doubleValue();
				}
				if (darr != null) {
					arrLiteral = new DoubleArrayLiteral(darr);
				}
			} else if (l.getCoreDatatype().asXSDDatatypeOrNull() == XSD.FLOAT) {
				float[] farr = new float[values.length];
				farr[0] = l.floatValue();
				for (int i=1; i<values.length; i++) {
					l = asLiteral(values[i]);
					if (l.getCoreDatatype().asXSDDatatypeOrNull() != XSD.FLOAT) {
						farr = null;
						break;
					}
					farr[i] = l.floatValue();
				}
				if (farr != null) {
					arrLiteral = new FloatArrayLiteral(farr);
				}
			}
			if (arrLiteral == null) {
				Object[] objs = new Object[values.length];
				Object obj = fromValue(values[0]);
				objs[0] = obj;
				Class<?> componentType = obj.getClass();
				for (int i=1; i<values.length; i++) {
					obj = fromValue(values[i]);
					objs[i] = obj;
					Class<?> nextType = obj.getClass();
					if (nextType != componentType) {
						componentType = Object.class;
					}
				}
				arrLiteral = new ObjectArrayLiteral(objs, componentType);
			}
		} else {
			arrLiteral = new ObjectArrayLiteral();
		}
		return arrLiteral;
	}

	private static Object fromValue(Value v) {
		Object o;
		Literal l = asLiteral(v);
		XSD xsd = l.getCoreDatatype().asXSDDatatypeOrNull();
		if (xsd != null) {
			try {
				switch (xsd) {
					case SHORT:
						// upcast to int
						o = l.intValue();
						break;
					case INT:
						o = l.intValue();
						break;
					case LONG:
						o = l.longValue();
						break;
					case FLOAT:
						o = l.floatValue();
						break;
					case DOUBLE:
						o = l.doubleValue();
						break;
					default:
						o = l.getLabel();
				}
			} catch (NumberFormatException nfe) {
				o = l.getLabel();
			}
		} else if (HALYARD.ARRAY_TYPE.equals(l.getDatatype())) {
			o = ObjectArrayLiteral.objectArray(l);
		} else if (HALYARD.MAP_TYPE.equals(l.getDatatype())) {
			o = MapLiteral.objectMap(l);
		} else {
			o = l.getLabel();
		}
		return o;
	}

	private static Literal asLiteral(Value v) {
		if (!v.isLiteral()) {
			throw new ValueExprEvaluationException(String.format("not a literal: %s", v));
		}
		return (Literal) v;
	}

	@Override
	public final IRI getDatatype() {
		return HalyardDatatype.ARRAY.getIri();
	}

	@Override
	public final CoreDatatype getCoreDatatype() {
		return HalyardDatatype.ARRAY;
	}

	public abstract Class<?> componentType();

	public abstract Object[] elements();

	public abstract int length();

	@Override
	public final boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof AbstractArrayLiteral) {
			AbstractArrayLiteral<?> other = (AbstractArrayLiteral<?>) o;
			return Arrays.equals(elements(), other.elements());
		} else {
			return super.equals(o);
		}
	}
}
