package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.optimizers.ExtendedBindingAssignerOptimizer;
import com.msd.gin.halyard.optimizers.ExtendedEvaluationStatistics;
import com.msd.gin.halyard.optimizers.ExtendedFilterOptimizer;
import com.msd.gin.halyard.optimizers.ExtendedIterativeEvaluationOptimizer;
import com.msd.gin.halyard.optimizers.ExtendedQueryModelNormalizer;
import com.msd.gin.halyard.optimizers.ExtendedUnionScopeChangeOptimizer;
import com.msd.gin.halyard.optimizers.QueryJoinOptimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ParentReferenceChecker;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.RegexAsStringFunctionOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.StandardQueryOptimizerPipeline;

public class ExtendedQueryOptimizerPipeline implements QueryOptimizerPipeline {
	private static boolean assertsEnabled = false;
	static final ExtendedBindingAssignerOptimizer BINDING_ASSIGNER = new ExtendedBindingAssignerOptimizer();
	static final ExtendedUnionScopeChangeOptimizer UNION_SCOPE_CHANGE_OPTIMIZER = new ExtendedUnionScopeChangeOptimizer();
	static final ExtendedQueryModelNormalizer QUERY_MODEL_NORMALIZER = new ExtendedQueryModelNormalizer();
	static final ExtendedFilterOptimizer FILTER_OPTIMIZER = new ExtendedFilterOptimizer();
	static final ExtendedIterativeEvaluationOptimizer ITERATIVE_EVALUATION_OPTIMIZER = new ExtendedIterativeEvaluationOptimizer();

	static {
		assert assertsEnabled = true;
	}

	private final ExtendedEvaluationStatistics statistics;
	private final EvaluationStrategy strategy;
	private final ValueFactory valueFactory;

	public ExtendedQueryOptimizerPipeline(EvaluationStrategy strategy, ValueFactory valueFactory, ExtendedEvaluationStatistics statistics) {
		this.strategy = strategy;
		this.valueFactory = valueFactory;
		this.statistics = statistics;
	}

	@Override
	public Iterable<QueryOptimizer> getOptimizers() {
		return check(Arrays.asList(
			BINDING_ASSIGNER,
			StandardQueryOptimizerPipeline.BINDING_SET_ASSIGNMENT_INLINER,
			new ConstantOptimizer(strategy),
			new RegexAsStringFunctionOptimizer(valueFactory),
			StandardQueryOptimizerPipeline.COMPARE_OPTIMIZER,
			StandardQueryOptimizerPipeline.CONJUNCTIVE_CONSTRAINT_SPLITTER,
			StandardQueryOptimizerPipeline.DISJUNCTIVE_CONSTRAINT_OPTIMIZER,
			StandardQueryOptimizerPipeline.SAME_TERM_FILTER_OPTIMIZER,
			UNION_SCOPE_CHANGE_OPTIMIZER,
			QUERY_MODEL_NORMALIZER,
			StandardQueryOptimizerPipeline.PROJECTION_REMOVAL_OPTIMIZER, // Make sure this is after the UnionScopeChangeOptimizer
			new QueryJoinOptimizer(statistics),
			ITERATIVE_EVALUATION_OPTIMIZER,
			FILTER_OPTIMIZER, // after join optimizer so we push down on the best statements
			StandardQueryOptimizerPipeline.ORDER_LIMIT_OPTIMIZER
		));
	}

	static Iterable<QueryOptimizer> check(Iterable<QueryOptimizer> optimizers) {
		if (assertsEnabled) {
			List<QueryOptimizer> optimizersWithReferenceCleaner = new ArrayList<>();
			optimizersWithReferenceCleaner.add(new ParentReferenceChecker(null));
			for (QueryOptimizer optimizer : optimizers) {
				optimizersWithReferenceCleaner.add(optimizer);
				optimizersWithReferenceCleaner.add(new ParentReferenceChecker(optimizer));
			}
			optimizers = optimizersWithReferenceCleaner;
		}
		return optimizers;
	}
}
