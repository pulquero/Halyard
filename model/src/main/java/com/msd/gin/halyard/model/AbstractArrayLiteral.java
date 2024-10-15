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

import com.msd.gin.halyard.model.vocabulary.HALYARD;

public abstract class AbstractArrayLiteral<T> extends AbstractDataLiteral implements ObjectLiteral<T> {
	private static final long serialVersionUID = -6423024672894102212L;

	public static boolean isArrayLiteral(Value v) {
		return v != null && v.isLiteral() && HALYARD.ARRAY_TYPE.equals(((Literal)v).getDatatype());
	}

	public static Value[] toValues(Object[] oarr, ValueFactory vf) {
		Value[] varr = new Value[oarr.length];
		for (int i=0; i<oarr.length; i++) {
			varr[i] = Values.literal(vf, oarr[i], false);
		}
		return varr;
	}

	public static AbstractArrayLiteral<?> createFromValues(Value[] values) {
		AbstractArrayLiteral<?> arrLiteral = null;
		if (values.length > 0) {
			Literal l = asLiteral(values[0]);
			if (l.getCoreDatatype().asXSDDatatypeOrNull() == XSD.FLOAT) {
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
				for (int i=0; i<values.length; i++) {
					objs[i] = fromValue(values[i]);
				}
				arrLiteral = new ObjectArrayLiteral(objs);
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
						o = l.shortValue();
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
		return HALYARD.ARRAY_TYPE;
	}

	@Override
	public final CoreDatatype getCoreDatatype() {
		return CoreDatatype.NONE;
	}

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
