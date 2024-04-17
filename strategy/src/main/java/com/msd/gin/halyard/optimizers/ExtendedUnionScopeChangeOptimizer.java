package com.msd.gin.halyard.optimizers;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.VariableScopeChange;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractSimpleQueryModelVisitor;

public final class ExtendedUnionScopeChangeOptimizer implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new UnionScopeChangeFixer());
	}

	private static class UnionScopeChangeFixer extends AbstractSimpleQueryModelVisitor<RuntimeException> {

		private UnionScopeChangeFixer() {
			super(true);
		}

		@Override
		public void meet(Union union) {
			super.meet(union);
			if (union.isVariableScopeChange()) {
				UnionArgChecker checker = new UnionArgChecker();
				union.getLeftArg().visit(checker);
				if (checker.containsBindOrValues) {
					return;
				}

				union.getRightArg().visit(checker);

				if (checker.containsBindOrValues) {
					return;
				}

				// Neither argument of the union contains a BIND or VALUES clause, we can safely ignore scope change
				// for binding injection
				union.setVariableScopeChange(false);
				setVariableScope(union.getLeftArg(), false);
				setVariableScope(union.getRightArg(), false);
			}
		}
	}

	private static void setVariableScope(QueryModelNode node, boolean changed) {
		if (node instanceof VariableScopeChange) {
			((VariableScopeChange)node).setVariableScopeChange(changed);
		}
	}

	private static class UnionArgChecker extends AbstractSimpleQueryModelVisitor<RuntimeException> {

		boolean containsBindOrValues = false;

		private UnionArgChecker() {
			super(false);
		}

		@Override
		public void meet(Union union) {
			if (!union.isVariableScopeChange()) {
				super.meet(union);
			}
		}

		@Override
		public void meet(Projection subselect) {
			// do not check deeper in the tree
		}

		@Override
		public void meet(Extension node) throws RuntimeException {
			containsBindOrValues = true;
		}

		@Override
		public void meet(BindingSetAssignment bsa) {
			containsBindOrValues = true;
		}
	}
}
