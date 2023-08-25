package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.Algorithms;
import com.msd.gin.halyard.algebra.SkipVarsQueryModelVisitor;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinAlgorithmOptimizer implements QueryOptimizer {
	private static final Logger LOGGER = LoggerFactory.getLogger(JoinAlgorithmOptimizer.class);
	private static double INDEX_SCAN_COST = 1.0;  // HBase scan cost
	private static double HASH_LOOKUP_COST = 0.001;
	private static double HASH_BUILD_COST = 0.005;

	private final ExtendedEvaluationStatistics statistics;
	private final int hashJoinLimit;
	private final float costRatio;

	public JoinAlgorithmOptimizer(ExtendedEvaluationStatistics stats, int hashJoinLimit, float ratio) {
		this.statistics = stats;
		this.hashJoinLimit = hashJoinLimit;
		this.costRatio = ratio;
	}

	public int getHashJoinLimit() {
		return hashJoinLimit;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new SkipVarsQueryModelVisitor<RuntimeException>() {
			@Override
			public void meet(Join join) {
				super.meet(join);
				selectJoinAlgorithm(join);
			}

			@Override
			public void meet(LeftJoin leftJoin) {
				super.meet(leftJoin);
				selectJoinAlgorithm(leftJoin);
			}
		});
	}

	private void selectJoinAlgorithm(BinaryTupleOperator join) {
		TupleExpr left = join.getLeftArg();
		TupleExpr right = join.getRightArg();
		if (isSupported(left) && isSupported(right)) {
			Set<String> boundVars = getBoundVars(join);
			double leftCard = statistics.getCardinality(left, boundVars, true);
			// calculate cardinality excluding bindings coming from the left
			double rightCard = statistics.getCardinality(right, boundVars, false);
			// nested loops: evaluate left, for each left bs, evaluate right (scan)
			double nestedCost = leftCard * INDEX_SCAN_COST;
			// hash join: evaluate right (scan), build hash, evaluate left, for each left bs, lookup right
			double hashCost = INDEX_SCAN_COST + rightCard * HASH_BUILD_COST + leftCard * HASH_LOOKUP_COST;
			boolean useHash = rightCard <= hashJoinLimit && costRatio*hashCost < nestedCost;
			LOGGER.debug("Nested join cost {} vs hash join cost {} ({}, {})", nestedCost, hashCost, leftCard, rightCard);
			if (useHash) {
				join.setAlgorithm(Algorithms.HASH_JOIN);
				join.setCostEstimate(hashCost);
			}
		}
	}

	/**
	 * NB: Hash-join only coincides with SPARQL semantics in a few special cases (e.g. no complex scoping).
	 * @param expr expression to check
	 * @return true if a hash-join can be used
	 */
	private static boolean isSupported(TupleExpr expr) {
		return (expr instanceof StatementPattern)
				|| (expr instanceof BindingSetAssignment)
				|| ((expr instanceof BinaryTupleOperator) && Algorithms.HASH_JOIN.equals(((BinaryTupleOperator)expr).getAlgorithmName()));
	}

	private Set<String> getBoundVars(TupleExpr node) {
		Set<String> vars = new HashSet<>();
		QueryModelNode parent = node.getParentNode();
		while (parent instanceof TupleExpr) {
			if (parent instanceof Join || parent instanceof LeftJoin) {
				BinaryTupleOperator op = (BinaryTupleOperator) parent;
				if (((BinaryTupleOperator) parent).getRightArg() == node) {
					vars.addAll(op.getLeftArg().getBindingNames());
				}
			}
			node = (TupleExpr) parent;
			parent = parent.getParentNode();
		}
		return vars;
	}
}
