package com.msd.gin.halyard.algebra.evaluation;

import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;

public class SimpleTupleFunctionContextFactory implements TupleFunctionContext.Factory {
	private final TripleSource ts;
	private final TupleFunctionRegistry tfRegistry;

	public SimpleTupleFunctionContextFactory(TripleSource ts, TupleFunctionRegistry tfRegistry) {
		this.ts = ts;
		this.tfRegistry = tfRegistry;
	}

	@Override
	public TupleFunctionContext create() {
		return new TupleFunctionContext() {
			@Override
			public TripleSource getTripleSource() {
				return ts;
			}

			@Override
			public void close() {
			}
		};
	}

	@Override
	public TupleFunctionRegistry getTupleFunctionRegistry() {
		return tfRegistry;
	}
}
