package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.model.AbstractArrayLiteral;
import com.msd.gin.halyard.model.DoubleArrayLiteral;
import com.msd.gin.halyard.model.FloatArrayLiteral;
import com.msd.gin.halyard.model.ObjectArrayLiteral;

import javax.annotation.concurrent.ThreadSafe;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.util.XMLDatatypeMathUtil;

@ThreadSafe
public class MathOpEvaluator {
	public Literal evaluate(Literal a, Literal b, MathOp op, ValueFactory vf) {
		try {
			return XMLDatatypeMathUtil.compute(a, b, op, vf);
		} catch (ValueExprEvaluationException ex) {
			AbstractArrayLiteral<?> avec = AbstractArrayLiteral.isArrayLiteral(a) ? AbstractArrayLiteral.asArrayLiteral(a) : null;
			AbstractArrayLiteral<?> bvec = AbstractArrayLiteral.isArrayLiteral(b) ? AbstractArrayLiteral.asArrayLiteral(b) : null;
			if (avec != null && bvec != null) {
				return operationBetweenVectors(avec, bvec, op, vf);
			} else if (avec != null && op == MathOp.DIVIDE) {
				CoreDatatype.XSD bcdt = b.getCoreDatatype().asXSDDatatypeOrNull();
				if (bcdt != null && bcdt.isNumericDatatype()) {
					return operationVectorDivideScalar(avec, b, op, vf);
				}
			} else if (bvec != null) {
				CoreDatatype.XSD acdt = a.getCoreDatatype().asXSDDatatypeOrNull();
				if (acdt != null && acdt.isNumericDatatype()) {
					return operationScalarMultiplyVector(a, bvec, op, vf);
				}
			}
			throw ex;
		}
	}

	private static AbstractArrayLiteral<?> operationBetweenVectors(AbstractArrayLiteral<?> a, AbstractArrayLiteral<?> b, MathOp op, ValueFactory vf) {
		if (a.length() != b.length()) {
			throw new ValueExprEvaluationException("Arrays have incompatible dimensions");
		}
		if ((a.componentType() == Double.class) || (b.componentType() == Double.class)) {
			double[] aarr = DoubleArrayLiteral.doubleArray(a);
			double[] barr = DoubleArrayLiteral.doubleArray(b);
			switch (op) {
				case PLUS:
					return new DoubleArrayLiteral(add(aarr, barr));
				case MINUS:
					return new DoubleArrayLiteral(subtract(aarr, barr));
				default:
					throw new AssertionError("Unsupported operator: " + op);
			}
		} else if ((a.componentType() == Float.class) || (b.componentType() == Float.class)) {
			float[] aarr = FloatArrayLiteral.floatArray(a);
			float[] barr = FloatArrayLiteral.floatArray(b);
			switch (op) {
				case PLUS:
					return new FloatArrayLiteral(add(aarr, barr));
				case MINUS:
					return new FloatArrayLiteral(subtract(aarr, barr));
				default:
					throw new AssertionError("Unsupported operator: " + op);
			}
		} else {
			Object[] aarr = ObjectArrayLiteral.objectArray(a);
			Object[] barr = ObjectArrayLiteral.objectArray(b);
			try {
				switch (op) {
					case PLUS:
						return AbstractArrayLiteral.createFromArray(add(aarr, barr));
					case MINUS:
						return AbstractArrayLiteral.createFromArray(subtract(aarr, barr));
					default:
						throw new AssertionError("Unsupported operator: " + op);
				}
			} catch (ClassCastException ex) {
				throw new ValueExprEvaluationException(ex);
			}
		}
	}

	private static double[] add(double[] a, double[] b) {
		double[] y = new double[a.length];
		for (int i=0; i<a.length; i++) {
			y[i] = a[i] + b[i];
		}
		return y;
	}

	private static double[] subtract(double[] a, double[] b) {
		double[] y = new double[a.length];
		for (int i=0; i<a.length; i++) {
			y[i] = a[i] - b[i];
		}
		return y;
	}

	private static float[] add(float[] a, float[] b) {
		float[] y = new float[a.length];
		for (int i=0; i<a.length; i++) {
			y[i] = a[i] + b[i];
		}
		return y;
	}

	private static float[] subtract(float[] a, float[] b) {
		float[] y = new float[a.length];
		for (int i=0; i<a.length; i++) {
			y[i] = a[i] - b[i];
		}
		return y;
	}

	private static Object[] add(Object[] a, Object[] b) {
		Object[] y = new Object[a.length];
		for (int i=0; i<a.length; i++) {
			if (a[i] instanceof Double || b[i] instanceof Double) {
				y[i] = ((Number) a[i]).doubleValue() + ((Number) b[i]).doubleValue();
			} else if (a[i] instanceof Float || b[i] instanceof Float) {
				y[i] = ((Number) a[i]).floatValue() + ((Number) b[i]).floatValue();
			} else if (a[i] instanceof Long || b[i] instanceof Long) {
				y[i] = ((Number) a[i]).longValue() + ((Number) b[i]).longValue();
			} else {
				y[i] = ((Number) a[i]).intValue() + ((Number) b[i]).intValue();
			}
		}
		return y;
	}

	private static Object[] subtract(Object[] a, Object[] b) {
		Object[] y = new Object[a.length];
		for (int i=0; i<a.length; i++) {
			if (a[i] instanceof Double || b[i] instanceof Double) {
				y[i] = ((Number) a[i]).doubleValue() - ((Number) b[i]).doubleValue();
			} else if (a[i] instanceof Float || b[i] instanceof Float) {
				y[i] = ((Number) a[i]).floatValue() - ((Number) b[i]).floatValue();
			} else if (a[i] instanceof Long || b[i] instanceof Long) {
				y[i] = ((Number) a[i]).longValue() - ((Number) b[i]).longValue();
			} else {
				y[i] = ((Number) a[i]).intValue() - ((Number) b[i]).intValue();
			}
		}
		return y;
	}

	private static AbstractArrayLiteral<?> operationScalarMultiplyVector(Literal scalar, AbstractArrayLiteral<?> vec, MathOp op, ValueFactory vf) {
		CoreDatatype.XSD sdt = scalar.getCoreDatatype().asXSDDatatype().get();
		if ((vec.componentType() == Double.class) || (sdt == CoreDatatype.XSD.DOUBLE)) {
			double[] v = DoubleArrayLiteral.doubleArray(vec);
			double s = scalar.doubleValue();
			double[] y = new double[v.length];
			for (int i=0; i<v.length; i++) {
				y[i] = s * v[i];
			}
			return new DoubleArrayLiteral(y);
		} else if ((vec.componentType() == Float.class) || (sdt == CoreDatatype.XSD.FLOAT)) {
			float[] v = FloatArrayLiteral.floatArray(vec);
			float s = scalar.floatValue();
			float[] y = new float[v.length];
			for (int i=0; i<v.length; i++) {
				y[i] = s * v[i];
			}
			return new FloatArrayLiteral(y);
		} else {
			Object[] arr = ObjectArrayLiteral.objectArray(vec);
			Object[] y = new Object[arr.length];
			try {
				for (int i=0; i<arr.length; i++) {
					if (sdt == CoreDatatype.XSD.DOUBLE || sdt == CoreDatatype.XSD.DECIMAL || arr[i] instanceof Double) {
						y[i] = scalar.doubleValue() * ((Number) arr[i]).doubleValue();
					} else if (sdt == CoreDatatype.XSD.FLOAT || arr[i] instanceof Float) {
						y[i] = scalar.floatValue() * ((Number) arr[i]).floatValue();
					} else if (sdt == CoreDatatype.XSD.LONG || sdt == CoreDatatype.XSD.INTEGER || arr[i] instanceof Long) {
						y[i] = scalar.longValue() * ((Number) arr[i]).longValue();
					} else {
						y[i] = scalar.intValue() * ((Number) arr[i]).intValue();
					}
				}
			} catch (ClassCastException ex) {
				throw new ValueExprEvaluationException(ex);
			}
			return AbstractArrayLiteral.createFromArray(y);
		}
	}

	private static AbstractArrayLiteral<?> operationVectorDivideScalar(AbstractArrayLiteral<?> vec, Literal scalar, MathOp op, ValueFactory vf) {
		CoreDatatype.XSD sdt = scalar.getCoreDatatype().asXSDDatatype().get();
		if ((vec.componentType() == Double.class) || (sdt == CoreDatatype.XSD.DOUBLE)) {
			double[] v = DoubleArrayLiteral.doubleArray(vec);
			double s = scalar.doubleValue();
			double[] y = new double[v.length];
			for (int i=0; i<v.length; i++) {
				y[i] = v[i] / s;
			}
			return new DoubleArrayLiteral(y);
		} else if ((vec.componentType() == Float.class) || (sdt == CoreDatatype.XSD.FLOAT)) {
			float[] v = FloatArrayLiteral.floatArray(vec);
			float s = scalar.floatValue();
			float[] y = new float[v.length];
			for (int i=0; i<v.length; i++) {
				y[i] = v[i] / s;
			}
			return new FloatArrayLiteral(y);
		} else {
			Object[] arr = ObjectArrayLiteral.objectArray(vec);
			Object[] y = new Object[arr.length];
			try {
				for (int i=0; i<arr.length; i++) {
					if (sdt == CoreDatatype.XSD.DOUBLE || sdt == CoreDatatype.XSD.DECIMAL || arr[i] instanceof Double) {
						y[i] = ((Number) arr[i]).doubleValue() / scalar.doubleValue();
					} else if (sdt == CoreDatatype.XSD.FLOAT || arr[i] instanceof Float) {
						y[i] = ((Number) arr[i]).floatValue() / scalar.floatValue();
					} else if (sdt == CoreDatatype.XSD.LONG || sdt == CoreDatatype.XSD.INTEGER || arr[i] instanceof Long) {
						y[i] = ((Number) arr[i]).doubleValue() / scalar.doubleValue();
					} else {
						y[i] = ((Number) arr[i]).floatValue() / scalar.floatValue();
					}
				}
			} catch (ClassCastException ex) {
				throw new ValueExprEvaluationException(ex);
			}
			return AbstractArrayLiteral.createFromArray(y);
		}
	}
}
