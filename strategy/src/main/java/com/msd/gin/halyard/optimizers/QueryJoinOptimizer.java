package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.NAryTupleOperator;
import com.msd.gin.halyard.algebra.Parent;
import com.msd.gin.halyard.algebra.StarJoin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TripleRef;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;

/**
 * A query optimizer that re-orders nested Joins.
 */
public class QueryJoinOptimizer implements QueryOptimizer {

	private final ExtendedEvaluationStatistics statistics;

	public QueryJoinOptimizer(ExtendedEvaluationStatistics statistics) {
		this.statistics = statistics;
	}

	/**
	 * Applies generally applicable optimizations: path expressions are sorted from more to less specific.
	 *
	 * @param tupleExpr expression to optimize
	 * @param dataset the dataset that will be used
	 * @param bindings any bindings that will be used
	 */
	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new JoinVisitor());
	}


	protected class JoinVisitor extends /*Halyard*/AbstractExtendedQueryModelVisitor<RuntimeException> {

		protected Set<String> boundVars = new HashSet<>();
		private boolean skipResultSizeEstimate = false;

		protected void updateCardinalityMap(TupleExpr tupleExpr, Map<TupleExpr,Double> cardinalityMap) {
			statistics.updateCardinalityMap(tupleExpr, boundVars, cardinalityMap, true);
		}

		private Map<TupleExpr,Double> getCardinalityMap(TupleExpr tupleExpr) {
			Map<TupleExpr, Double> map = new HashMap<>();
			updateCardinalityMap(tupleExpr, map);
			return map;
		}

		private void setResultSizeEstimates(Map<TupleExpr,Double> cardinalityMap) {
			if (!skipResultSizeEstimate) {
				cardinalityMap.entrySet().stream().forEach(entry -> {
					TupleExpr expr = entry.getKey();
					double cardinality = entry.getValue();
					expr.setResultSizeEstimate(cardinality);
				});
			}
		}

		private void setResultSizeEstimate(TupleExpr tupleExpr) {
			if (!skipResultSizeEstimate) {
				Map<TupleExpr,Double> cardinalityMap = getCardinalityMap(tupleExpr);
				setResultSizeEstimates(cardinalityMap);
			}
		}

		@Override
		public void meet(StatementPattern node) {
			setResultSizeEstimate(node);
		}

		@Override
		public void meet(TripleRef node) {
			setResultSizeEstimate(node);
		}

		@Override
		protected void meetUnaryTupleOperator(UnaryTupleOperator node) {
			super.meetUnaryTupleOperator(node);
			setResultSizeEstimate(node);
		}

		@Override
		protected void meetBinaryTupleOperator(BinaryTupleOperator node) {
			super.meetBinaryTupleOperator(node);
			setResultSizeEstimate(node);
		}

		@Override
		protected void meetNAryTupleOperator(NAryTupleOperator node) {
			super.meetNAryTupleOperator(node);
			setResultSizeEstimate(node);
		}

		@Override
		public void meet(LeftJoin leftJoin) {
			leftJoin.getLeftArg().visit(this);

			Set<String> origBoundVars = boundVars;
			try {
				boundVars = new HashSet<>(origBoundVars);
				boundVars.addAll(leftJoin.getLeftArg().getBindingNames());

				leftJoin.getRightArg().visit(this);
			} finally {
				boundVars = origBoundVars;
			}
		}

        @Override
        public void meet(StarJoin node) {
        	List<? extends TupleExpr> sjArgs = node.getArgs();
        	Parent parent = Parent.wrap(node);
        	// replace the star join with a join tree.
        	Join joins = (Join) Algebra.join(sjArgs);
        	node.replaceWith(joins);
        	// optimize the join order
        	meet(joins);
        	// re-attach in new order
        	List<TupleExpr> orderedArgs = new ArrayList<>(sjArgs.size());
    		parent.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>() {
    			@Override
    			public void meet(Join join) {
    				TupleExpr left = join.getLeftArg();
    				TupleExpr right = join.getRightArg();
    				// joins should be right-recursive
    				assert !(left instanceof Join);
   					orderedArgs.add(left);
   					if (!(right instanceof Join)) {
   						// leaf join has both left and right
   						orderedArgs.add(right);
   					}
   					right.visit(this);
    			}

    			@Override
    			public void meet(StatementPattern node) {
    				// skip children
    			}

    			@Override
    			public void meet(TripleRef node) {
    				// skip children
    			}
			});
        	node.setArgs(orderedArgs);
        	parent.replaceWith(node);
        }


        private void optimizePriorityJoin(Set<String> origBoundVars, TupleExpr join) {
			Set<String> saveBoundVars = boundVars;
			try {
				boundVars = new HashSet<>(origBoundVars);
				join.visit(this);
			} finally {
				boundVars = saveBoundVars;
			}
		}

		@Override
		public void meet(Join node) {
			TupleExpr replacement;
			Set<String> origBoundVars = boundVars;
			try {
				boundVars = new HashSet<>(origBoundVars);

				// Recursively get the join arguments
				List<TupleExpr> joinArgs = getJoinArgs(node, new ArrayList<>());

				// get all extensions (BIND clause)
				List<TupleExpr> orderedExtensions = getExtensionTupleExprs(joinArgs);
				joinArgs.removeAll(orderedExtensions);

				// get all subselects and order them
				List<TupleExpr> orderedSubselects = reorderSubselects(getSubSelects(joinArgs));
				joinArgs.removeAll(orderedSubselects);

				// Reorder the subselects and extensions to a more optimal sequence
				List<TupleExpr> priorityArgs;
				if (orderedExtensions.isEmpty()) {
					priorityArgs = orderedSubselects;
				} else if (orderedSubselects.isEmpty()) {
					priorityArgs = orderedExtensions;
				} else {
					priorityArgs = new ArrayList<>(orderedExtensions.size() + orderedSubselects.size());
					priorityArgs.addAll(orderedExtensions);
					priorityArgs.addAll(orderedSubselects);
				}

				// Build new join hierarchy
				TupleExpr priorityJoins = null;
				Set<String> priorityBounds = null;
				if (!priorityArgs.isEmpty()) {
					priorityBounds = new HashSet<>();
					TupleExpr expr = priorityArgs.get(0);
					priorityBounds.addAll(expr.getBindingNames());
					priorityJoins = expr;
					for (int i = 1; i < priorityArgs.size(); i++) {
						expr = priorityArgs.get(i);
						priorityBounds.addAll(expr.getBindingNames());
						priorityJoins = new Join(priorityJoins, expr);
					}
				}

				// We order all remaining join arguments based on cardinality and
				// variable frequency statistics
				if (!joinArgs.isEmpty()) {
					// Reorder the (recursive) join arguments to a more optimal sequence
					List<TupleExpr> orderedJoinArgs = new ArrayList<>(joinArgs.size());

					// Build map of vars per tuple expression
					Map<TupleExpr, List<Var>> varsMap = new HashMap<>();
					for (TupleExpr tupleExpr : joinArgs) {
						if (tupleExpr instanceof Join) {
							// we can skip calculating the cardinality for instances of Join since we will anyway "meet"
							// these nodes
							continue;
						}

						if (tupleExpr instanceof ZeroLengthPath) {
							varsMap.put(tupleExpr, ((ZeroLengthPath) tupleExpr).getVarList());
						} else {
							varsMap.put(tupleExpr, getStatementPatternVars(tupleExpr));
						}
					}

					// Build map of var frequences
					Map<Var, Integer> varFreqMap = new HashMap<>((varsMap.size() + 1) * 2);
					for (List<Var> varList : varsMap.values()) {
						fillVarFreqMap(varList, varFreqMap);
					}

					if (priorityBounds != null) {
						boundVars.addAll(priorityBounds);
					}
					// order all other join arguments based on available statistics
					while (!joinArgs.isEmpty()) {
						TupleExpr[] oldNewExpr = selectNextTupleExpr(joinArgs, varsMap, varFreqMap);
						TupleExpr oldExpr = oldNewExpr[0];
						TupleExpr newExpr = oldNewExpr[1];
						joinArgs.remove(oldExpr);
						orderedJoinArgs.add(newExpr);

						boundVars.addAll(newExpr.getBindingNames());
					}

					// Note: generated hierarchy is right-recursive to help the
					// IterativeEvaluationOptimizer to factor out the left-most join
					// argument
					int i = orderedJoinArgs.size() - 1;
					replacement = orderedJoinArgs.get(i);
					for (i--; i >= 0; i--) {
						TupleExpr arg = orderedJoinArgs.get(i);
						Join join = new Join(arg, replacement);
						replacement = join;
					}

					if (priorityJoins != null) {
						replacement = new Join(priorityJoins, replacement);
					}

					// Replace old join hierarchy
					node.replaceWith(replacement);

					// we optimize after the replacement call above in case the optimize call below
					// recurses back into this function and we need all the node's parent/child pointers
					// set up correctly for replacement to work on subsequent calls
					if (priorityJoins != null) {
						optimizePriorityJoin(origBoundVars, priorityJoins);
					}
				} else {
					// only subselect/priority joins involved in this query.
					replacement = priorityJoins;
					node.replaceWith(replacement);
				}

				setResultSizeEstimate(replacement);
			} finally {
				boundVars = origBoundVars;
			}
		}

		private <L extends List<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr, L joinArgs) {
			if (tupleExpr instanceof Join) {
				Join join = (Join) tupleExpr;
				getJoinArgs(join.getLeftArg(), joinArgs);
				getJoinArgs(join.getRightArg(), joinArgs);
			} else {
				joinArgs.add(tupleExpr);
			}

			return joinArgs;
		}

		private List<Var> getStatementPatternVars(TupleExpr tupleExpr) {
			if (tupleExpr instanceof StatementPattern) {
				return ((StatementPattern) tupleExpr).getVarList();
			}

			if (tupleExpr instanceof BindingSetAssignment) {
				return List.of();
			}

			return new StatementPatternVarCollector(tupleExpr).getVars();
		}

		private <M extends Map<Var, Integer>> void fillVarFreqMap(List<Var> varList, M varFreqMap) {
			if (varList.isEmpty()) {
				return;
			}

			for (Var var : varList) {
				varFreqMap.compute(var, (k, v) -> {
					if (v == null) {
						return 1;
					}
					return v + 1;
				});
			}
		}

		private List<TupleExpr> getExtensionTupleExprs(List<TupleExpr> expressions) {
			if (expressions.isEmpty())
				return List.of();

			List<TupleExpr> extensions = List.of();
			for (TupleExpr expr : expressions) {
				if (TupleExprs.containsExtension(expr)) {
					if (extensions.isEmpty()) {
						extensions = List.of(expr);
					} else {
						if (extensions.size() == 1) {
							extensions = new ArrayList<>(extensions);
						}
						extensions.add(expr);
					}
				}
			}
			return extensions;
		}

		private List<TupleExpr> getSubSelects(List<TupleExpr> expressions) {
			if (expressions.isEmpty())
				return List.of();

			List<TupleExpr> subselects = List.of();
			for (TupleExpr expr : expressions) {
				if (TupleExprs.containsSubquery(expr)) {
					if (subselects.isEmpty()) {
						subselects = List.of(expr);
					} else {
						if (subselects.size() == 1) {
							subselects = new ArrayList<>(subselects);
						}
						subselects.add(expr);
					}
				}
			}
			return subselects;
		}

		/**
		 * Determines an optimal ordering of subselect join arguments, based on variable bindings. An ordering is
		 * considered optimal if for each consecutive element it holds that first of all its shared variables with all
		 * previous elements is maximized, and second, the union of all its variables with all previous elements is
		 * maximized.
		 * <p>
		 * Example: reordering
		 *
		 * <pre>
		 *   [f] [a b c] [e f] [a d] [b e]
		 * </pre>
		 * <p>
		 * should result in:
		 *
		 * <pre>
		 *   [a b c] [a d] [b e] [e f] [f]
		 * </pre>
		 *
		 * @param subSelects the original ordering of expressions
		 * @return the optimized ordering of expressions
		 */
		private List<TupleExpr> reorderSubselects(List<TupleExpr> subSelects) {

			if (subSelects.size() == 1) {
				return subSelects;
			}

			List<TupleExpr> result = new ArrayList<>();
			if (subSelects.isEmpty()) {
				return result;
			}

			// Step 1: determine size of join for each pair of arguments
			HashMap<Integer, List<TupleExpr[]>> joinSizes = new HashMap<>();

			int maxJoinSize = 0;
			for (int i = 0; i < subSelects.size(); i++) {
				TupleExpr firstArg = subSelects.get(i);
				for (int j = i + 1; j < subSelects.size(); j++) {
					TupleExpr secondArg = subSelects.get(j);

					int joinSize = getJoinSize(firstArg.getBindingNames(), secondArg.getBindingNames());

					if (joinSize > maxJoinSize) {
						maxJoinSize = joinSize;
					}

					List<TupleExpr[]> l;

					if (joinSizes.containsKey(joinSize)) {
						l = joinSizes.get(joinSize);
					} else {
						l = new ArrayList<>();
					}
					TupleExpr[] tupleTuple = new TupleExpr[] { firstArg, secondArg };
					l.add(tupleTuple);
					joinSizes.put(joinSize, l);
				}
			}

			// Step 2: find the first two elements for the ordered list by
			// selecting the pair with first of all,
			// the highest join size, and second, the highest union size.

			TupleExpr[] maxUnionTupleTuple = null;
			int currentUnionSize = -1;

			// get a list of all argument pairs with the maximum join size
			List<TupleExpr[]> list = joinSizes.get(maxJoinSize);

			// select the pair that has the highest union size.
			for (TupleExpr[] tupleTuple : list) {
				Set<String> names = tupleTuple[0].getBindingNames();
				names.addAll(tupleTuple[1].getBindingNames());
				int unionSize = names.size();

				if (unionSize > currentUnionSize) {
					maxUnionTupleTuple = tupleTuple;
					currentUnionSize = unionSize;
				}
			}

			// add the pair to the result list.
			assert maxUnionTupleTuple != null;
			result.add(maxUnionTupleTuple[0]);
			result.add(maxUnionTupleTuple[1]);

			// Step 3: sort the rest of the list by selecting and adding an element
			// at a time.
			while (result.size() < subSelects.size()) {
				result.add(getNextSubselect(result, subSelects));
			}

			return result;
		}

		private TupleExpr getNextSubselect(List<TupleExpr> currentList, List<TupleExpr> joinArgs) {

			// determine union of names of all elements currently in the list: this
			// corresponds to the projection resulting from joining all these
			// elements.
			Set<String> currentListNames = new HashSet<>();
			for (TupleExpr expr : currentList) {
				currentListNames.addAll(expr.getBindingNames());
			}

			// select the next argument from the list, by checking that it has,
			// first, the highest join size with the current list, and second, the
			// highest union size.
			TupleExpr selected = null;
			int currentUnionSize = -1;
			int currentJoinSize = -1;
			for (TupleExpr candidate : joinArgs) {
				if (!currentList.contains(candidate)) {

					Set<String> names = candidate.getBindingNames();
					int joinSize = getJoinSize(currentListNames, names);

					Set<String> candidateBindingNames = candidate.getBindingNames();
					int unionSize = getUnionSize(currentListNames, candidateBindingNames);

					if (joinSize > currentJoinSize) {
						selected = candidate;
						currentJoinSize = joinSize;
						currentUnionSize = unionSize;
					} else if (joinSize == currentJoinSize) {
						if (unionSize > currentUnionSize) {
							selected = candidate;
							currentUnionSize = unionSize;
						}
					}
				}
			}

			return selected;
		}

		/**
		 * Selects from a list of tuple expressions the next tuple expression that should be evaluated. This method
		 * selects the tuple expression with highest number of bound variables, preferring variables that have been
		 * bound in other tuple expressions over variables with a fixed value.
		 * @param expressions expressions to order
		 * @param varsMap vars used by the expressions
		 * @param varFreqMap var frequency
		 * @return the next expression (original and new form) that should be evaluated
		 */
		private TupleExpr[] selectNextTupleExpr(List<TupleExpr> expressions,
				Map<TupleExpr, List<Var>> varsMap, Map<Var, Integer> varFreqMap) {
			if (expressions.size() == 1) {
				TupleExpr tupleExpr = expressions.get(0);
				TupleExpr optimizedExpr = optimizeTupleExpr(tupleExpr);
				Map<TupleExpr,Double> cardinalityMap = getCardinalityMap(optimizedExpr);
				double cardinality = cardinalityMap.get(optimizedExpr);
				setResultSizeEstimates(cardinalityMap);
				optimizedExpr.setCostEstimate(getTupleExprCost(tupleExpr, optimizedExpr, cardinality, varsMap, varFreqMap));
				return new TupleExpr[] {tupleExpr, optimizedExpr};
			}

			TupleExpr resultNew = null;
			TupleExpr resultOld = null;
			Map<TupleExpr,Double> resultCardinalityMap = null;
			double lowestCost = Double.POSITIVE_INFINITY;

			for (TupleExpr tupleExpr : expressions) {
				// optimize the TupleExpr given the current bounds
				TupleExpr optimizedExpr = optimizeTupleExpr(tupleExpr);
				// Calculate a score for this tuple expression
				Map<TupleExpr,Double> cardinalityMap = getCardinalityMap(optimizedExpr);
				double cardinality = cardinalityMap.get(optimizedExpr);
				double cost = getTupleExprCost(tupleExpr, optimizedExpr, cardinality, varsMap, varFreqMap);

				if (cost < lowestCost || resultNew == null) {
					// More specific path expression found
					lowestCost = cost;
					resultNew = optimizedExpr;
					resultOld = tupleExpr;
					resultCardinalityMap = cardinalityMap;
					if (cost == 0.0) {
						break;
					}
				}
			}

			assert resultNew != null;
			setResultSizeEstimates(resultCardinalityMap);
			resultNew.setCostEstimate(lowestCost);

			return new TupleExpr[] {resultOld, resultNew};
		}

		private TupleExpr optimizeTupleExpr(TupleExpr tupleExpr) {
			if (Algebra.isTupleOperator(tupleExpr)) {
				boolean currentSkipResultSizeEstimate = skipResultSizeEstimate;
				// trial optimization so don't store any result size estimates
				skipResultSizeEstimate = true;
				try {
					Parent parent = Parent.wrap(tupleExpr);
					tupleExpr.visit(this);
					return Parent.unwrap(parent);
				} finally {
					skipResultSizeEstimate = currentSkipResultSizeEstimate;
				}
			} else {
				return tupleExpr;
			}
		}

		private double getTupleExprCost(TupleExpr tupleExpr, TupleExpr optimizedExpr, double cardinality,
				Map<TupleExpr, List<Var>> varsMap, Map<Var, Integer> varFreqMap) {

			// BindingSetAssignment has a typical constant cost. This cost is not based on statistics so is much more
			// reliable. If the BindingSetAssignment binds to any of the other variables in the other tuple expressions
			// to choose from, then the cost of the BindingSetAssignment should be set to 0 since it will always limit
			// the upper bound of any other costs. This way the BindingSetAssignment will be chosen as the left
			// argument.
			if (optimizedExpr instanceof BindingSetAssignment) {

				Set<Var> varsUsedInOtherExpressions = varFreqMap.keySet();

				for (String assuredBindingName : optimizedExpr.getAssuredBindingNames()) {
					if (varsUsedInOtherExpressions.contains(new Var(assuredBindingName))) {
						return 0.0;
					}
				}
				return 1.0;
			}

			double cost = cardinality;

			List<Var> vars = varsMap.get(tupleExpr);

			// Compensate for variables that are bound earlier in the evaluation
			List<Var> unboundVars = getUnboundVars(vars);
			int constantVars = countConstantVars(vars);

			int nonConstantVarCount = vars.size() - constantVars;

			if (nonConstantVarCount > 0) {
				double exp = (double) unboundVars.size() / nonConstantVarCount;
				cost = Math.pow(cost, exp);
			}

			if (unboundVars.isEmpty()) {
				// Prefer patterns with more bound vars
				if (nonConstantVarCount > 0) {
					cost /= nonConstantVarCount;
				}
			} else {
				// Prefer patterns that bind variables from other tuple expressions
				int foreignVarFreq = getForeignVarFreq(unboundVars, varFreqMap);
				if (foreignVarFreq > 0) {
					cost /= 1 + foreignVarFreq;
				}
			}

			return cost;
		}

		private int countConstantVars(List<Var> vars) {
			int size = 0;

			for (Var var : vars) {
				if (var.hasValue()) {
					size++;
				}
			}

			return size;
		}

		private List<Var> getUnboundVars(List<Var> vars) {
			int size = vars.size();
			if (size == 0) {
				return List.of();
			}
			if (size == 1) {
				Var var = vars.get(0);
				if (!var.hasValue() && var.getName() != null && !boundVars.contains(var.getName())) {
					return List.of(var);
				} else {
					return List.of();
				}
			}

			List<Var> ret = null;

			for (Var var : vars) {
				if (!var.hasValue() && var.getName() != null && !boundVars.contains(var.getName())) {
					if (ret == null) {
						ret = List.of(var);
					} else {
						if (ret.size() == 1) {
							ret = new ArrayList<>(ret);
						}
						ret.add(var);
					}
				}
			}

			return ret != null ? ret : Collections.emptyList();
		}

		private int getForeignVarFreq(List<Var> ownUnboundVars, Map<Var, Integer> varFreqMap) {
			if (ownUnboundVars.isEmpty()) {
				return 0;
			}
			if (ownUnboundVars.size() == 1) {
				return varFreqMap.get(ownUnboundVars.get(0)) - 1;
			} else {
				int result = -ownUnboundVars.size();
				for (Var var : new HashSet<>(ownUnboundVars)) {
					result += varFreqMap.get(var);
				}
				return result;

			}
		}
	}

	private static int getUnionSize(Set<String> currentListNames, Set<String> candidateBindingNames) {
		int count = 0;
		for (String n : currentListNames) {
			if (!candidateBindingNames.contains(n)) {
				count++;
			}
		}
		return candidateBindingNames.size() + count;
	}

	private static int getJoinSize(Set<String> currentListNames, Set<String> names) {
		int count = 0;
		for (String name : names) {
			if (currentListNames.contains(name)) {
				count++;
			}
		}
		return count;
	}


	private static class StatementPatternVarCollector extends StatementPatternVisitor {

		private final TupleExpr tupleExpr;
		private List<Var> vars;

		public StatementPatternVarCollector(TupleExpr tupleExpr) {
			this.tupleExpr = tupleExpr;
		}

		@Override
		protected void accept(StatementPattern node) {
			if (vars == null) {
				vars = new ArrayList<>(node.getVarList());
			} else {
				vars.addAll(node.getVarList());
			}
		}

		public List<Var> getVars() {
			if (vars == null) {
				try {
					tupleExpr.visit(this);
				} catch (Exception e) {
					if (e instanceof InterruptedException) {
						Thread.currentThread().interrupt();
					}
					throw new IllegalStateException(e);
				}
				if (vars == null) {
					vars = Collections.emptyList();
				}
			}

			return vars;
		}
	}
}
