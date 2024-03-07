package com.msd.gin.halyard.query.algebra;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TripleRef;

public abstract class SkipVarsQueryModelVisitor<X extends Exception> extends AbstractExtendedQueryModelVisitor<X> {
	@Override
	public void meet(StatementPattern node) {
		// skip children
	}

	@Override
	public void meet(TripleRef node) {
		// skip children
	}
}
