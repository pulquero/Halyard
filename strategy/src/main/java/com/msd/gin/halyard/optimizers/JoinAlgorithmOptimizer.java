package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.HashJoin;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class JoinAlgorithmOptimizer implements QueryOptimizer {

	private final EvaluationStatistics statistics;
	private final int hashJoinLimit;

	public JoinAlgorithmOptimizer(EvaluationStatistics stats, int hashJoinLimit) {
		this.statistics = stats;
		this.hashJoinLimit = hashJoinLimit;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			@Override
			public void meet(Join join) throws RuntimeException {
				TupleExpr left = join.getLeftArg();
				TupleExpr right = join.getRightArg();
				// get cardinalities assuming no bound variables as nothing additionally will be bound with a hash join
				double leftCard = statistics.getCardinality(left);
				double rightCard = statistics.getCardinality(right);
				if (leftCard <= hashJoinLimit && rightCard >= 2*leftCard) {
					// hash left
					join.setAlgorithm(HashJoin.INSTANCE);
					join.setCostEstimate(0.1);
					left.setResultSizeEstimate(leftCard);
					right.setResultSizeEstimate(rightCard);
					// need to swap args
					join.setLeftArg(right);
					join.setRightArg(left);
				} else if (rightCard <= hashJoinLimit && leftCard >= 2*rightCard) {
					// hash right
					join.setAlgorithm(HashJoin.INSTANCE);
					join.setCostEstimate(0.1);
					left.setResultSizeEstimate(leftCard);
					right.setResultSizeEstimate(rightCard);
				}
				super.meet(join);
			}
		});
	}
}
