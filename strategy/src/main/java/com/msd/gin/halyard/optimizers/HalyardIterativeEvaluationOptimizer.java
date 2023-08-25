package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.SkipVarsQueryModelVisitor;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;

public final class HalyardIterativeEvaluationOptimizer implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new IEOVisitor());
	}

	private static class IEOVisitor extends SkipVarsQueryModelVisitor<RuntimeException> {
		@Override
		public void meet(Union union) {
			super.meet(union);

			TupleExpr leftArg = union.getLeftArg();
			TupleExpr rightArg = union.getRightArg();

			if (leftArg instanceof Join && rightArg instanceof Join) {
				Join leftJoinArg = (Join) leftArg;
				Join rightJoin = (Join) rightArg;

				if (leftJoinArg.getLeftArg().equals(rightJoin.getLeftArg())) {
					// factor out the left-most join argument
					TupleExpr commonFactor = leftJoinArg.getLeftArg();
					Join newJoin = new Join();
					union.replaceWith(newJoin);
					newJoin.setLeftArg(commonFactor);
					newJoin.setRightArg(union);
					union.setLeftArg(leftJoinArg.getRightArg());
					union.setRightArg(rightJoin.getRightArg());
					// Halyard
					// re-estimate
					double commonEstimate = commonFactor.getResultSizeEstimate();
					double unionEstimate = union.getResultSizeEstimate();
					if (commonEstimate > 0.0 && unionEstimate >= 0.0) {
						union.setResultSizeEstimate(unionEstimate/commonEstimate);
						newJoin.setResultSizeEstimate(unionEstimate);
					} else {
						union.setResultSizeEstimate(-1);
					}

					union.visit(this);
				}
			}
		}
	}
}
