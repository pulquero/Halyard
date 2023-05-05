package com.msd.gin.halyard.optimizers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

import org.eclipse.rdf4j.query.algebra.Var;

public interface StatementPatternCardinalityCalculator extends Closeable {

	public static interface Factory {
		StatementPatternCardinalityCalculator create() throws IOException;
	}

	double getStatementCardinality(Var subjVar, Var predVar, Var objVar, Var ctxVar, Collection<String> boundVars);
	double getTripleCardinality(Var subjVar, Var predVar, Var objVar, Collection<String> boundVars);
}
