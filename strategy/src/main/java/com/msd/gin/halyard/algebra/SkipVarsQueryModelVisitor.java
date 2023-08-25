package com.msd.gin.halyard.algebra;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TripleRef;

public abstract class SkipVarsQueryModelVisitor<X extends Exception> extends AbstractExtendedQueryModelVisitor<X> {
	@Override
	public final void meet(StatementPattern node) {
		// skip children
	}

	@Override
	public final void meet(TripleRef node) {
		// skip children
	}
}
