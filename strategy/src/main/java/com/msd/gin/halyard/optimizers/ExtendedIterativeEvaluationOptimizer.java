package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.query.algebra.NAryUnion;
import com.msd.gin.halyard.query.algebra.SkipVarsQueryModelVisitor;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;

public final class ExtendedIterativeEvaluationOptimizer implements QueryOptimizer {

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
			if ((leftArg instanceof Join) && (rightArg instanceof Join)) {
				Join leftJoinArg = (Join) leftArg;
				Join rightJoinArg = (Join) rightArg;
				TupleExpr newLeftSide = leftJoinArg.getRightArg();
				TupleExpr newRightSide = rightJoinArg.getRightArg();
				if (!TupleExprs.isVariableScopeChange(newLeftSide) && !TupleExprs.isVariableScopeChange(newRightSide)) {
					TupleExpr commonFactor = leftJoinArg.getLeftArg();
					if (commonFactor.equals(rightJoinArg.getLeftArg())) {
						// factor out the left-most join argument
						Join newJoin = new Join();
						union.replaceWith(newJoin);
						newJoin.setLeftArg(commonFactor);
						newJoin.setRightArg(union);
						union.setLeftArg(newLeftSide);
						union.setRightArg(newRightSide);

						adjustVariableScopes(union, newJoin);
						reestimateResultSizes(union, commonFactor, newJoin);
	
						union.visit(this);
					}
				}
			}
		}

		private void adjustVariableScopes(AbstractQueryModelNode union, Join newJoin) {
			if (union.isVariableScopeChange()) {
				newJoin.setVariableScopeChange(true);
				union.setVariableScopeChange(false);
			}
		}

		private void reestimateResultSizes(AbstractQueryModelNode union, TupleExpr commonFactor, Join newJoin) {
			double commonEstimate = commonFactor.getResultSizeEstimate();
			double unionEstimate = union.getResultSizeEstimate();
			if (commonEstimate > 0.0 && unionEstimate >= 0.0) {
				union.setResultSizeEstimate(unionEstimate/commonEstimate);
				newJoin.setResultSizeEstimate(unionEstimate);
			} else {
				union.setResultSizeEstimate(-1);
			}
		}

		@Override
		public void meet(NAryUnion union) {
			super.meet(union);

			TupleExpr firstArg = union.getArg(0);
			if (firstArg instanceof Join) {
				Join firstJoin = (Join) firstArg;
				TupleExpr commonFactor = firstJoin.getLeftArg();
				TupleExpr newLeftSide = firstJoin.getRightArg();
				if (!TupleExprs.isVariableScopeChange(newLeftSide)) {
					int n = union.getArgCount();
					List<TupleExpr> newArgs = new ArrayList<>(n);
					newArgs.add(newLeftSide);
					for (int i=1; i<n; i++) {
						TupleExpr arg = union.getArg(i);
						if (!(arg instanceof Join)) {
							return;
						}
						Join join = (Join) arg;
						TupleExpr newArg = join.getRightArg();
						if (commonFactor.equals(join.getLeftArg()) && !TupleExprs.isVariableScopeChange(newArg)) {
							newArgs.add(newArg);
						} else {
							return;
						}
					}

					// factor out the left-most join argument
					Join newJoin = new Join();
					union.replaceWith(newJoin);
					newJoin.setLeftArg(commonFactor);
					newJoin.setRightArg(union);
					union.setArgs(newArgs);

					adjustVariableScopes(union, newJoin);
					reestimateResultSizes(union, commonFactor, newJoin);

					union.visit(this);
				}
			}
		}
	}
}
