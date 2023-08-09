package com.msd.gin.halyard.algebra;

import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

public class ExtendedQueryRoot extends QueryRoot {
	private static final long serialVersionUID = -4974644197084310816L;

	public ExtendedQueryRoot(TupleExpr expr) {
		super(expr);
	}

	@Override
	public String toString() {
		ExtendedQueryModelTreePrinter treePrinter = new ExtendedQueryModelTreePrinter();
		this.visit(treePrinter);
		return treePrinter.getTreeString();
	}
}
