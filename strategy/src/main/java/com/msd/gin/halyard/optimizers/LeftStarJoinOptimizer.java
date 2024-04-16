package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.LeftStarJoin;
import com.msd.gin.halyard.query.algebra.SkipVarsQueryModelVisitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.VarNameCollector;

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
			while(!current.hasCondition() && isWellDesigned(current)) {
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


	private static boolean isWellDesigned(LeftJoin leftJoin) {
		VarNameCollector optionalVarCollector = new VarNameCollector();
		leftJoin.getRightArg().visit(optionalVarCollector);
		if (leftJoin.hasCondition()) {
			leftJoin.getCondition().visit(optionalVarCollector);
		}

		Set<String> leftBindingNames = leftJoin.getLeftArg().getBindingNames();
		Set<String> problemVars = removeAll(optionalVarCollector.getVarNames(), leftBindingNames);

		if (problemVars.isEmpty()) {
			return true;
		}

		return checkAgainstParent(leftJoin, problemVars);
	}

	private static Set<String> removeAll(Set<String> problemVars, Set<String> leftBindingNames) {
		if (!leftBindingNames.isEmpty() && !problemVars.isEmpty()) {
			if (leftBindingNames.size() > problemVars.size()) {
				for (String problemVar : problemVars) {
					if (leftBindingNames.contains(problemVar)) {
						HashSet<String> ret = new HashSet<>(problemVars);
						ret.removeAll(leftBindingNames);
						return ret;
					}
				}
			} else {
				for (String leftBindingName : leftBindingNames) {
					if (problemVars.contains(leftBindingName)) {
						HashSet<String> ret = new HashSet<>(problemVars);
						ret.removeAll(leftBindingNames);
						return ret;
					}
				}
			}
		}
		return problemVars;
	}

	private static boolean checkAgainstParent(LeftJoin leftJoin, Set<String> problemVars) {
		// If any of the problematic variables are bound in the parent
		// expression then the left join is not well designed
		BindingCollector bindingCollector = new BindingCollector();
		QueryModelNode node = leftJoin;
		QueryModelNode parent;
		while ((parent = node.getParentNode()) != null) {
			bindingCollector.setNodeToIgnore(node);
			parent.visitChildren(bindingCollector);
			node = parent;
		}

		Set<String> bindingNames = bindingCollector.getBindingNames();

		for (String problemVar : problemVars) {
			if (bindingNames.contains(problemVar)) {
				return false;
			}
		}

		return true;
	}

	private static class BindingCollector extends AbstractQueryModelVisitor<RuntimeException> {

		private QueryModelNode nodeToIgnore;

		private final Set<String> bindingNames = new HashSet<>();

		public void setNodeToIgnore(QueryModelNode node) {
			this.nodeToIgnore = node;
		}

		public Set<String> getBindingNames() {
			return bindingNames;
		}

		@Override
		protected void meetNode(QueryModelNode node) {
			if (node instanceof TupleExpr && node != nodeToIgnore) {
				TupleExpr tupleExpr = (TupleExpr) node;
				bindingNames.addAll(tupleExpr.getBindingNames());
			}
		}
	}
}
