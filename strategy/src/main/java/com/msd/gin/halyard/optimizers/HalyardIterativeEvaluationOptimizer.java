package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.SkipVarsQueryModelVisitor;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;

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
				TupleExpr leftSide = leftJoinArg.getRightArg();
				TupleExpr rightSide = rightJoin.getRightArg();

				if (leftJoinArg.getLeftArg().equals(rightJoin.getLeftArg()) && !(TupleExprs.isVariableScopeChange(leftSide) && TupleExprs.isVariableScopeChange(rightSide))) {
					// factor out the left-most join argument
					TupleExpr commonFactor = leftJoinArg.getLeftArg();
					Join newJoin = new Join();
					union.replaceWith(newJoin);
					newJoin.setLeftArg(commonFactor);
					newJoin.setRightArg(union);
					union.setLeftArg(leftSide);
					union.setRightArg(rightSide);
					// Halyard
					if (union.isVariableScopeChange()) {
						newJoin.setVariableScopeChange(true);
						union.setVariableScopeChange(false);
					}
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
