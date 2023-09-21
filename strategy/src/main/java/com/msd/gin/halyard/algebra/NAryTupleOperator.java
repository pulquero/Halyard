package com.msd.gin.halyard.algebra;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

public abstract class NAryTupleOperator extends AbstractQueryModelNode implements TupleExpr {
	private static final long serialVersionUID = -3171973153588379214L;
	private TupleExpr[] args;
	private String algorithmName;

	public void setArgs(List<? extends TupleExpr> exprs) {
		args = new TupleExpr[exprs.size()];
		for (int i=0; i<exprs.size(); i++) {
			TupleExpr te = exprs.get(i);
			setArg(i, te);
		}
	}

	private void setArg(int i, TupleExpr te) {
		te.setParentNode(this);
		args[i] = te;
	}

	public TupleExpr getArg(int i) {
		return args[i];
	}

	public List<? extends TupleExpr> getArgs() {
		return Arrays.asList(args);
	}

	public int getArgCount() {
		return args.length;
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		visitor.meetOther(this);
	}

	@Override
	public <X extends Exception> void visitChildren(final QueryModelVisitor<X> visitor) throws X {
		for (TupleExpr arg : args) {
			arg.visit(visitor);
		}
	}

	@Override
	public void replaceChildNode(final QueryModelNode current, final QueryModelNode replacement) {
		for (int i=0; i<args.length; i++) {
			if (current == args[i]) {
				setArg(i, (TupleExpr) replacement);
				return;
			}
		}
	}

	public void removeChildNode(TupleExpr current) {
		TupleExpr[] newArgs = new TupleExpr[args.length-1];
		boolean removed = false;
		for (int i=0, j=0; i<args.length; i++) {
			if (current == args[i]) {
				removed = true;
			} else if (j < newArgs.length) {
				newArgs[j++] = args[i];
			}
		}
		if (removed) {
			args = newArgs;
		}
	}

	@Override
	public Set<String> getBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<>(16);
		for (TupleExpr arg : args) {
			bindingNames.addAll(arg.getBindingNames());
		}
		return bindingNames;
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<>(16);
		for (TupleExpr arg : args) {
			bindingNames.addAll(arg.getAssuredBindingNames());
		}
		return bindingNames;
	}

	public void setAlgorithm(String classSimpleName) {
		this.algorithmName = classSimpleName;
	}

	public String getAlgorithmName() {
		return algorithmName;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof NAryTupleOperator) {
			NAryTupleOperator o = (NAryTupleOperator) other;
			return getArgs().equals(o.getArgs());
		}

		return false;
	}

	@Override
	public int hashCode() {
		if (args.length == 0) {
			return 0;
		}
		// NB: must use XOR so this can still be retrieved from a hash map after its args have been re-ordered
		int hash = args[0].hashCode();
		for (int i=1; i<args.length; i++) {
			hash ^= args[i].hashCode();
		}
		return hash;
	}

	@Override
	public NAryTupleOperator clone() {
		NAryTupleOperator clone = (NAryTupleOperator) super.clone();
		clone.args = new TupleExpr[args.length];
		for (int i=0; i<args.length; i++) {
			TupleExpr exprClone = args[i].clone();
			clone.setArg(i, exprClone);
		}

		return clone;
	}

	@Override
	public String toString() {
		ExtendedQueryModelTreePrinter treePrinter = new ExtendedQueryModelTreePrinter();
		this.visit(treePrinter);
		return treePrinter.getTreeString();
	}
}
