package com.msd.gin.halyard.algebra;

import java.util.List;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;

public final class Algebra {
	private Algebra() {}

	public static UnaryTupleOperator ensureRooted(TupleExpr tupleExpr) {
		if (!(tupleExpr instanceof QueryRoot)) {
			tupleExpr = new ExtendedQueryRoot(tupleExpr);
		}
		return (UnaryTupleOperator) tupleExpr;
	}

	public static boolean isEmpty(TupleExpr tupleExpr) {
		if (tupleExpr instanceof QueryRoot) {
			tupleExpr = ((QueryRoot) tupleExpr).getArg();
		}
		return (tupleExpr instanceof SingletonSet);
	}

	public static boolean isTupleOperator(TupleExpr tupleExpr) {
		return (tupleExpr instanceof UnaryTupleOperator) || isBranchTupleOperator(tupleExpr);
	}

	public static boolean isBranchTupleOperator(TupleExpr tupleExpr) {
		return (tupleExpr instanceof BinaryTupleOperator) || (tupleExpr instanceof NAryTupleOperator);
	}

	/**
	 * Removes a subtree.
	 * @param expr node and descendants to be removed.
	 */
	public static void remove(TupleExpr expr) {
		QueryModelNode parent = expr.getParentNode();
		if (parent instanceof Join) {
			Join join = (Join) parent;
			if (join.getLeftArg() == expr) {
				join.replaceWith(join.getRightArg());
			} else if (join.getRightArg() == expr) {
				join.replaceWith(join.getLeftArg());
			} else {
				throw new IllegalArgumentException(String.format("Corrupt join: %s", join));
			}
		} else if (parent instanceof Union) {
			Union union = (Union) parent;
			if (union.getLeftArg() == expr) {
				union.replaceWith(union.getRightArg());
			} else if (union.getRightArg() == expr) {
				union.replaceWith(union.getLeftArg());
			} else {
				throw new IllegalArgumentException(String.format("Corrupt union: %s", union));
			}
		} else if (parent instanceof UnaryTupleOperator) {
			expr.replaceWith(new SingletonSet());
		} else if (parent instanceof NAryTupleOperator) {
			NAryTupleOperator op = (NAryTupleOperator) parent;
			op.removeChildNode(expr);
			if (op.getArgCount() == 1) {
				op.replaceWith(op.getArg(0));
			}
		} else {
			throw new IllegalArgumentException(String.format("Cannot remove %s from %s", expr.getSignature(), parent.getSignature()));
		}
	}

	/**
	 * Builds a right-recursive join tree.
	 * @param exprs list of expressions to join.
	 * @return join tree containing the given expressions.
	 */
	public static TupleExpr join(List<? extends TupleExpr> exprs) {
		int i = exprs.size()-1;
		TupleExpr te = exprs.get(i);
		for (i--; i>=0; i--) {
			te = new Join(exprs.get(i), te);
		}
		return te;
	}

	public static UnaryTupleOperator compose(UnaryTupleOperator op1, UnaryTupleOperator op2, TupleExpr expr) {
		op2.setArg(expr);
		op1.setArg(op2);
		return op1;
	}

	public static Var createAnonVar(String varName) {
		return new Var(varName, true);
	}

	public static boolean isFree(Var var) {
		return !var.isAnonymous() && !var.hasValue();
	}

	/**
	 * Gets a value from a {@link Var} if it has a {@link Value}. If it does not then the method will attempt to get it
	 * from the bindings using the name of the Var
	 * @param var variable to get the value of.
	 * @param bindings bindings to use if variable doesn't have an existing value.
	 * @return the matching {@link Value} or {@code null} if var is {@code null}
	 */
	public static Value getVarValue(Var var, BindingSet bindings) {
	    if (var == null) {
	        return null;
	    }
	    Value v = var.getValue();
	    if (v == null) {
	        v = bindings.getValue(var.getName());
	    }
	    return v;
	}

	public static Value evaluateConstant(ValueExpr expr, BindingSet bindings) {
		Value v;
		if (expr instanceof ValueConstant) {
			v = ((ValueConstant) expr).getValue();
		} else if (expr instanceof Var) {
			v = Algebra.getVarValue((Var) expr, bindings);
		} else {
			v = null;
		}
		return v;
	}

	public static void _initResultSizeActual(QueryModelNode queryModelNode) {
		queryModelNode.setResultSizeActual(Math.max(0, queryModelNode.getResultSizeActual()));
	}

	public static void _incrementResultSizeActual(QueryModelNode queryModelNode, long delta) {
		queryModelNode.setResultSizeActual(queryModelNode.getResultSizeActual() + delta);
	}

	public static void _initTotalTimeNanosActual(QueryModelNode queryModelNode) {
		queryModelNode.setTotalTimeNanosActual(Math.max(0, queryModelNode.getTotalTimeNanosActual()));
	}

	public static void _incrementTotalTimeNanosActual(QueryModelNode queryModelNode, long delta) {
		queryModelNode.setTotalTimeNanosActual(queryModelNode.getTotalTimeNanosActual() + delta);
	}

	public static void initResultSizeActual(QueryModelNode queryModelNode) {
		synchronized (queryModelNode) {
			_initResultSizeActual(queryModelNode);
		}
	}

	public static void incrementResultSizeActual(QueryModelNode queryModelNode, long delta) {
		synchronized (queryModelNode) {
			_incrementResultSizeActual(queryModelNode, delta);
		}
	}

	public static void initTotalTimeNanosActual(QueryModelNode queryModelNode) {
		synchronized (queryModelNode) {
			_initTotalTimeNanosActual(queryModelNode);
		}
	}

	public static void incrementTotalTimeNanosActual(QueryModelNode queryModelNode, long delta) {
		synchronized (queryModelNode) {
			_incrementTotalTimeNanosActual(queryModelNode, delta);
		}
	}
}
