package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.model.FloatArrayLiteral;
import com.msd.gin.halyard.model.ObjectArrayLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

import javax.annotation.concurrent.ThreadSafe;

import org.eclipse.rdf4j.model.IRI;
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
			IRI adt = a.getDatatype();
			IRI bdt = b.getDatatype();
			boolean aisvec = HALYARD.ARRAY_TYPE.equals(adt);
			boolean bisvec = HALYARD.ARRAY_TYPE.equals(bdt);
			if (aisvec) {
				if (bisvec) {
					return operationBetweenVectors(a, b, op, vf);
				} else if (op == MathOp.DIVIDE) {
					CoreDatatype.XSD bcdt = b.getCoreDatatype().asXSDDatatypeOrNull();
					if (bcdt != null && bcdt.isNumericDatatype()) {
						return operationVectorDivideScalar(a, b, op, vf);
					}
				}
			} else if (bisvec) {
				if (aisvec) {
					return operationBetweenVectors(a, b, op, vf);
				} else if (op == MathOp.MULTIPLY) {
					CoreDatatype.XSD acdt = a.getCoreDatatype().asXSDDatatypeOrNull();
					if (acdt != null && acdt.isNumericDatatype()) {
						return operationScalarMultiplyVector(a, b, op, vf);
					}
				}
			}
			throw ex;
		}
	}

	private static Literal operationBetweenVectors(Literal a, Literal b, MathOp op, ValueFactory vf) {
		if ((a instanceof FloatArrayLiteral) && (b instanceof FloatArrayLiteral)) {
			float[] aarr = ((FloatArrayLiteral) a).objectValue();
			float[] barr = ((FloatArrayLiteral) b).objectValue();
			if (aarr.length != barr.length) {
				throw new ValueExprEvaluationException("Arrays have incompatible dimensions");
			}
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
			if (aarr.length != barr.length) {
				throw new ValueExprEvaluationException("Arrays have incompatible dimensions");
			}
			try {
				switch (op) {
					case PLUS:
						return new ObjectArrayLiteral(add(aarr, barr));
					case MINUS:
						return new ObjectArrayLiteral(subtract(aarr, barr));
					default:
						throw new AssertionError("Unsupported operator: " + op);
				}
			} catch (ClassCastException ex) {
				throw new ValueExprEvaluationException(ex);
			}
		}
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

	private static Literal operationScalarMultiplyVector(Literal scalar, Literal vec, MathOp op, ValueFactory vf) {
		CoreDatatype.XSD sdt = scalar.getCoreDatatype().asXSDDatatype().get();
		if ((sdt == CoreDatatype.XSD.FLOAT) && (vec instanceof FloatArrayLiteral)) {
			float s = scalar.floatValue();
			float[] v = ((FloatArrayLiteral) vec).objectValue();
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
			return new ObjectArrayLiteral(y);
		}
	}

	private static Literal operationVectorDivideScalar(Literal vec, Literal scalar, MathOp op, ValueFactory vf) {
		CoreDatatype.XSD sdt = scalar.getCoreDatatype().asXSDDatatype().get();
		if ((vec instanceof FloatArrayLiteral) && (sdt == CoreDatatype.XSD.FLOAT)) {
			float[] v = ((FloatArrayLiteral) vec).objectValue();
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
			return new ObjectArrayLiteral(y);
		}
	}
}
