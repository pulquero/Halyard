package com.msd.gin.halyard.query.algebra;

import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;

/**
 * Useful node for mid-tree surgery.
 */
public final class Parent extends UnaryTupleOperator {
	private static final long serialVersionUID = 1620947330539139269L;

	public static Parent wrap(TupleExpr node) {
		Parent parent = new Parent();
		node.replaceWith(parent);
		parent.setArg(node);
		return parent;
	}

	public static TupleExpr unwrap(Parent parent) {
		TupleExpr expr = parent.getArg();
		parent.replaceWith(expr);
		return expr;
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		visitor.meetOther(this);
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof Parent && super.equals(other);
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ "Parent".hashCode();
	}

	@Override
	public Parent clone() {
		return (Parent) super.clone();
	}
}