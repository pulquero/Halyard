package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.LeftStarJoin;
import com.msd.gin.halyard.query.algebra.SkipVarsQueryModelVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;

public class LeftStarJoinOptimizer implements QueryOptimizer {
	private final int minJoins;

	public LeftStarJoinOptimizer(int minJoins) {
		if (minJoins < 1) {
			throw new IllegalArgumentException("Minimum joins must be greater than or equal to one");
		}
		this.minJoins = minJoins;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new LeftStarJoinFinder());
	}

	final class LeftStarJoinFinder extends SkipVarsQueryModelVisitor<RuntimeException> {

		@Override
		public void meet(LeftJoin node) {
			List<StatementPattern> sps = new ArrayList<>();
			LeftJoin current = node;
			TupleExpr base = null;
			while(!current.hasCondition() && Algebra.isWellDesigned(current)) {
				TupleExpr rightArg = current.getRightArg();
				if (rightArg instanceof StatementPattern) {
					sps.add((StatementPattern) rightArg);
					base = current.getLeftArg();
					if (base instanceof LeftJoin) {
						current = (LeftJoin) base;
						continue;
					}
				}
				break;
			}

			if(!sps.isEmpty()) {
				processLeftJoins(base, sps);
			}
		}

		private void processLeftJoins(TupleExpr base, List<StatementPattern> sps) {
			List<StatementPattern> args = new ArrayList<>();
			// NB: statements are in reverse order!!!
			StatementPattern first = sps.get(sps.size()-1);
			args.add(first);
			StatementPattern.Scope scope = first.getScope();
			Var ctxVar = first.getContextVar();
			Var commonVar = first.getSubjectVar();
			for (int i=sps.size()-2; i>=0; i--) {
				StatementPattern sp = sps.get(i);
				if (sp.getScope() == scope && Objects.equals(sp.getContextVar(), ctxVar) && sp.getSubjectVar().equals(commonVar)) {
					args.add(sp);
				} else {
					break;
				}
			}
			if (args.size() >= minJoins) {
				for(StatementPattern sp : args) {
					Algebra.remove(sp);
				}
				LeftStarJoin lsj = new LeftStarJoin(scope, commonVar.clone(), ctxVar != null ? ctxVar.clone() : null, base.clone(), args);
				base.replaceWith(lsj);
			}
		}
	}
}
