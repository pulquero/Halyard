package com.msd.gin.halyard.optimizers;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.BGPCollector;
import com.msd.gin.halyard.query.algebra.Parent;
import com.msd.gin.halyard.query.algebra.SkipVarsQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.StarJoin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;

public class StarJoinOptimizer implements QueryOptimizer {
	private final int minJoins;

	public StarJoinOptimizer(int minJoins) {
		if (minJoins < 1) {
			throw new IllegalArgumentException("Minimum joins must be greater than or equal to one");
		}
		this.minJoins = minJoins;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new StarJoinFinder());
	}

	final class StarJoinFinder extends SkipVarsQueryModelVisitor<RuntimeException> {

		@Override
		public void meet(Join node) {
			BGPCollector<RuntimeException> collector = new BGPCollector<>(this);
			node.visit(collector);
			if(!collector.getStatementPatterns().isEmpty()) {
				Parent parent = Parent.wrap(node);
				processJoins(parent, collector.getStatementPatterns());
				Parent.unwrap(parent);
			}
		}

		private void processJoins(Parent parent, List<StatementPattern> sps) {
			ListMultimap<Triple<StatementPattern.Scope,Var,Var>, StatementPattern> spByCtxSubj = ArrayListMultimap.create(sps.size(), 4);
			for(StatementPattern sp : sps) {
				Var ctxVar = sp.getContextVar();
				Var subjVar = sp.getSubjectVar();
				spByCtxSubj.put(Triple.of(sp.getScope(), ctxVar, subjVar), sp);
			}

			for(Map.Entry<Triple<StatementPattern.Scope,Var,Var>, Collection<StatementPattern>> entry : spByCtxSubj.asMap().entrySet()) {
				List<StatementPattern> subjSps = (List<StatementPattern>) entry.getValue();
				// (num of joins) = (num of statement patterns) - 1
				if(subjSps.size() > minJoins) {
					for(StatementPattern sp : subjSps) {
						Algebra.remove(sp);
					}
					StatementPattern.Scope scope = entry.getKey().getLeft();
					Var ctxVar = entry.getKey().getMiddle();
					Var commonVar = entry.getKey().getRight();
					StarJoin sj = new StarJoin(scope, commonVar.clone(), ctxVar != null ? ctxVar.clone() : null, subjSps);
					TupleExpr top = parent.getArg();
					if (top instanceof SingletonSet) {
						top.replaceWith(sj);
					} else {
						Join combined = new Join();
						top.replaceWith(combined);
						combined.setLeftArg(top);
						combined.setRightArg(sj);
					}
				}
			}
		}
	}
}
