package com.msd.gin.halyard.sail;

import java.util.function.Consumer;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.SailConnection;

public interface BindingSetConsumerSailConnection extends SailConnection {
	default void evaluate(Consumer<BindingSet> handler, final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) {
		report(evaluate(tupleExpr, dataset, bindings, includeInferred), handler);
	}

	static void report(CloseableIteration<? extends BindingSet, QueryEvaluationException> iter, Consumer<BindingSet> handler) {
		while (iter.hasNext()) {
			handler.accept(iter.next());
		}
	}
}
