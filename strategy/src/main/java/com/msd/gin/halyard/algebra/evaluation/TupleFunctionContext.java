package com.msd.gin.halyard.algebra.evaluation;

import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;

public interface TupleFunctionContext extends AutoCloseable {
	public static interface Factory {
		TupleFunctionContext create();
		TupleFunctionRegistry getTupleFunctionRegistry();
	}

	TripleSource getTripleSource();

	@Override
	void close();
}
