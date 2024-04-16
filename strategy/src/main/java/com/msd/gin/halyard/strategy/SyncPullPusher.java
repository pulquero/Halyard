package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.query.BindingSetPipe;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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
			TupleExpr node, BindingSet bs,
			Function<CloseableIteration<BindingSet, QueryEvaluationException>,CloseableIteration<BindingSet, QueryEvaluationException>> trackerFactory) {
		active.incrementAndGet();
		try {
			pullPushAll(pipe, evalStep, node, bs, trackerFactory);
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
			TupleExpr expr, BindingSet bindingSet,
			Function<CloseableIteration<BindingSet, QueryEvaluationException>,CloseableIteration<BindingSet, QueryEvaluationException>> trackerFactory) {
		if (!pipe.isClosed()) {
			try {
				CloseableIteration<BindingSet, QueryEvaluationException> iter = trackerFactory.apply(evalStep.evaluate(bindingSet));
				boolean doNext = true;
				while (doNext && !pipe.isClosed()) {
		    		try {
		    			doNext = iter.hasNext();
		    			if (doNext) {
		        			BindingSet bs = iter.next();
		        			doNext = pipe.push(bs);
		    			}
		    		} catch (Throwable nextEx) {
		    			doNext = pipe.handleException(nextEx);
		    		}
				}
		        // close iter first to immediately release resources as pipe.close() maybe non-trivial (e.g. DISTINCT)
				try {
					iter.close();
				} catch (QueryEvaluationException ignore) {}
			} catch (Throwable evalEx) {
    			pipe.handleException(evalEx);
    		} finally {
    			pipe.close();
    		}
		}
	}
}
