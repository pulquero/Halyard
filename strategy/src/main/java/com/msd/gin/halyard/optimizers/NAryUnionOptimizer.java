package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.NAryUnion;
import com.msd.gin.halyard.algebra.SkipVarsQueryModelVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;

public class NAryUnionOptimizer implements QueryOptimizer {
	private final int minUnions;

	public NAryUnionOptimizer(int minUnions) {
		if (minUnions < 1) {
			throw new IllegalArgumentException("Minimum unions must be greater than or equal to one");
		}
		this.minUnions = minUnions;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new NAryUnionJoinFinder());
	}

	final class NAryUnionJoinFinder extends SkipVarsQueryModelVisitor<RDF4JException> {
		@Override
		public void meet(Union node) throws RDF4JException {
			UnionCollector<RDF4JException> collector = new UnionCollector<>(this);
			node.visit(collector);
			if (!collector.getExpressions().isEmpty()) {
				processUnions(node, collector.getExpressions());
			}
		}

		private void processUnions(Union top, List<TupleExpr> exprs) {
			if (exprs.size() > minUnions) {
				NAryUnion nunion = new NAryUnion(exprs);
				nunion.setVariableScopeChange(top.isVariableScopeChange());
				top.replaceWith(nunion);
			}
		}
	}

	static final class UnionCollector<X extends Exception> extends AbstractExtendedQueryModelVisitor<X> {
		private final QueryModelVisitor<X> visitor;
		private List<TupleExpr> exprs;

		UnionCollector(QueryModelVisitor<X> visitor) {
			this.visitor = visitor;
		}

		List<TupleExpr> getExpressions() {
			return (exprs != null) ? exprs : Collections.emptyList();
		}

		void addExpression(TupleExpr expr) throws X {
			if (exprs == null) {
				exprs = new ArrayList<>();
			}
			exprs.add(expr);
			// resume previous visitor
			expr.visit(visitor);
		}

		@Override
		public void meet(Union node) throws X {
			TupleExpr left = node.getLeftArg();
			TupleExpr right = node.getRightArg();
			if (left instanceof Union) {
				left.visit(this);
			} else {
				addExpression(left);
			}
			if (right instanceof Union) {
				right.visit(this);
			} else {
				addExpression(right);
			}
		}
	}
}
