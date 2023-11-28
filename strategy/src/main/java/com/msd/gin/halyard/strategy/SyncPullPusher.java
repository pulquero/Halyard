package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.query.BindingSetPipe;

import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;

final class SyncPullPusher implements PullPusher {
	private final AtomicInteger active = new AtomicInteger();

	@Override
	public void pullPush(BindingSetPipe pipe,
			QueryEvaluationStep evalStep,
			TupleExpr node, BindingSet bs, HalyardEvaluationStrategy strategy) {
		active.incrementAndGet();
		try {
			pullPushAll(pipe, evalStep, node, bs, strategy);
		} finally {
			active.decrementAndGet();
		}
	}

	@Override
	public int getActiveCount() {
		return active.get();
	}

	@Override
	public int getQueueSize() {
		return 0;
	}

	@Override
	public void close() {
	}

	static void pullPushAll(BindingSetPipe pipe,
			QueryEvaluationStep evalStep,
			TupleExpr expr, BindingSet bindingSet, HalyardEvaluationStrategy strategy) {
		if (!pipe.isClosed()) {
			CloseableIteration<BindingSet, QueryEvaluationException> iter = strategy.track(evalStep.evaluate(bindingSet), expr);
			boolean doNext = true;
			while (doNext && !pipe.isClosed()) {
	    		try {
	    			doNext = iter.hasNext();
	    			if (doNext) {
	        			BindingSet bs = iter.next();
	        			doNext = pipe.push(bs);
	    			}
	    		} catch (Throwable e) {
	    			doNext = pipe.handleException(e);
	    		}
			}
	        // close iter first to immediately release resources as pipe.close() maybe non-trivial (e.g. DISTINCT)
			try {
				iter.close();
			} catch (QueryEvaluationException ignore) {
			}
			pipe.close();
		}
	}
}
