package com.msd.gin.halyard.query.algebra;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.SubQueryValueOperator;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.VarNameCollector;

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
		} else if (parent instanceof LeftJoin) {
			LeftJoin leftJoin = (LeftJoin) parent;
			if (leftJoin.getRightArg() == expr) {
				leftJoin.replaceWith(leftJoin.getLeftArg());
			} else {
				throw new IllegalArgumentException(String.format("Cannot remove %s from %s", expr.getSignature(), parent.getSignature()));
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
		} else if (parent instanceof StarJoin || parent instanceof NAryUnion) {
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

	public static boolean isPartOfSubQuery(QueryModelNode node) {
		if (node instanceof SubQueryValueOperator) {
			return true;
		}

		QueryModelNode parent = node.getParentNode();
		if (parent == null) {
			return false;
		} else {
			return isPartOfSubQuery(parent);
		}
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

	public static boolean isWellDesigned(LeftJoin leftJoin) {
		VarNameCollector optionalVarCollector = new VarNameCollector();
		leftJoin.getRightArg().visit(optionalVarCollector);
		if (leftJoin.hasCondition()) {
			leftJoin.getCondition().visit(optionalVarCollector);
		}

		Set<String> leftBindingNames = leftJoin.getLeftArg().getBindingNames();
		Set<String> problemVars = removeAll(optionalVarCollector.getVarNames(), leftBindingNames);

		if (problemVars.isEmpty()) {
			return true;
		}

		return checkAgainstParent(leftJoin, problemVars);
	}

	private static Set<String> removeAll(Set<String> problemVars, Set<String> leftBindingNames) {
		if (!leftBindingNames.isEmpty() && !problemVars.isEmpty()) {
			if (leftBindingNames.size() > problemVars.size()) {
				for (String problemVar : problemVars) {
					if (leftBindingNames.contains(problemVar)) {
						HashSet<String> ret = new HashSet<>(problemVars);
						ret.removeAll(leftBindingNames);
						return ret;
					}
				}
			} else {
				for (String leftBindingName : leftBindingNames) {
					if (problemVars.contains(leftBindingName)) {
						HashSet<String> ret = new HashSet<>(problemVars);
						ret.removeAll(leftBindingNames);
						return ret;
					}
				}
			}
		}
		return problemVars;
	}

	private static boolean checkAgainstParent(LeftJoin leftJoin, Set<String> problemVars) {
		// If any of the problematic variables are bound in the parent
		// expression then the left join is not well designed
		BindingCollector bindingCollector = new BindingCollector();
		QueryModelNode node = leftJoin;
		QueryModelNode parent;
		while ((parent = node.getParentNode()) != null) {
			bindingCollector.setNodeToIgnore(node);
			parent.visitChildren(bindingCollector);
			node = parent;
		}

		Set<String> bindingNames = bindingCollector.getBindingNames();

		for (String problemVar : problemVars) {
			if (bindingNames.contains(problemVar)) {
				return false;
			}
		}

		return true;
	}

	private static class BindingCollector extends AbstractQueryModelVisitor<RuntimeException> {

		private QueryModelNode nodeToIgnore;

		private final Set<String> bindingNames = new HashSet<>();

		public void setNodeToIgnore(QueryModelNode node) {
			this.nodeToIgnore = node;
		}

		public Set<String> getBindingNames() {
			return bindingNames;
		}

		@Override
		protected void meetNode(QueryModelNode node) {
			if (node instanceof TupleExpr && node != nodeToIgnore) {
				TupleExpr tupleExpr = (TupleExpr) node;
				bindingNames.addAll(tupleExpr.getBindingNames());
			}
		}
	}
}
