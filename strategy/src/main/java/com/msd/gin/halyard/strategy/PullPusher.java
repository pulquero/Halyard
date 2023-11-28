package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.query.BindingSetPipe;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;

interface PullPusher extends AutoCloseable {
	void pullPush(BindingSetPipe pipe,
			QueryEvaluationStep evalStep,
			TupleExpr node, BindingSet bs, HalyardEvaluationStrategy strategy);
	int getActiveCount();
	int getQueueSize();
	@Override
	void close() throws RuntimeException;
}
