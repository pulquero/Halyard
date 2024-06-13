package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.SailConnection;

import com.msd.gin.halyard.query.BindingSetPipe;

public interface BindingSetPipeSailConnection extends SailConnection {
	default void evaluate(BindingSetPipe pipe, final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) {
		report(evaluate(tupleExpr, dataset, bindings, includeInferred), pipe);
		pipe.close();
	}

	/**
	 * NB: does not close the pipe so you can report multiple iterations to the same pipe.
	 * @param iter iteration of binding sets to push to the pipe
	 * @param pipe the pipe to push to
	 * @return true if the pipe can accept more binding sets
	 */
	static boolean report(CloseableIteration<? extends BindingSet> iter, BindingSetPipe pipe) {
		while (iter.hasNext()) {
			if (!pipe.push(iter.next())) {
				return false;
			}
		}
		return true;
	}
}
