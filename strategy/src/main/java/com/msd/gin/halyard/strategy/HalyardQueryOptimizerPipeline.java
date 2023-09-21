/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.optimizers.ConstrainedValueOptimizer;
import com.msd.gin.halyard.optimizers.ExtendedEvaluationStatistics;
import com.msd.gin.halyard.optimizers.HalyardConstantOptimizer;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.HalyardQueryJoinOptimizer;
import com.msd.gin.halyard.optimizers.JoinAlgorithmOptimizer;
import com.msd.gin.halyard.optimizers.QueryJoinOptimizer;
import com.msd.gin.halyard.optimizers.StarJoinOptimizer;

import java.util.Arrays;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.RegexAsStringFunctionOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.StandardQueryOptimizerPipeline;

/**
*
* @author Adam Sotona
*/
public final class HalyardQueryOptimizerPipeline implements QueryOptimizerPipeline {
	private final ExtendedEvaluationStatistics statistics;
	private final EvaluationStrategy strategy;
	private final ValueFactory valueFactory;
	private final StarJoinOptimizer starJoinOptimizer;
	private final JoinAlgorithmOptimizer joinAlgoOptimizer;

	public HalyardQueryOptimizerPipeline(HalyardEvaluationStrategy strategy, ValueFactory valueFactory, ExtendedEvaluationStatistics statistics) {
		this.strategy = strategy;
		this.valueFactory = valueFactory;
		this.statistics = statistics;
		int minJoins = strategy.getConfig().starJoinMinJoins;
		this.starJoinOptimizer = new StarJoinOptimizer(minJoins);
		int hashJoinLimit = strategy.getConfig().hashJoinLimit;
		float costRatio = strategy.getConfig().hashJoinCostRatio;
		this.joinAlgoOptimizer = new JoinAlgorithmOptimizer(statistics, hashJoinLimit, costRatio);
	}

	JoinAlgorithmOptimizer getJoinAlgorithmOptimizer() {
		return joinAlgoOptimizer;
	}

	@Override
	public Iterable<QueryOptimizer> getOptimizers() {
		return ExtendedQueryOptimizerPipeline.check(Arrays.asList(
			ExtendedQueryOptimizerPipeline.BINDING_ASSIGNER,
			StandardQueryOptimizerPipeline.BINDING_SET_ASSIGNMENT_INLINER,
			new HalyardConstantOptimizer(strategy),
			new RegexAsStringFunctionOptimizer(valueFactory),
			StandardQueryOptimizerPipeline.COMPARE_OPTIMIZER,
			StandardQueryOptimizerPipeline.CONJUNCTIVE_CONSTRAINT_SPLITTER,
			StandardQueryOptimizerPipeline.DISJUNCTIVE_CONSTRAINT_OPTIMIZER,
			StandardQueryOptimizerPipeline.SAME_TERM_FILTER_OPTIMIZER,
			StandardQueryOptimizerPipeline.UNION_SCOPE_CHANGE_OPTIMIZER,
			StandardQueryOptimizerPipeline.QUERY_MODEL_NORMALIZER,
			StandardQueryOptimizerPipeline.PROJECTION_REMOVAL_OPTIMIZER, // Make sure this is after the UnionScopeChangeOptimizer
			new ConstrainedValueOptimizer(),
			starJoinOptimizer,
			(statistics instanceof HalyardEvaluationStatistics) ? new HalyardQueryJoinOptimizer((HalyardEvaluationStatistics) statistics) : new QueryJoinOptimizer(statistics),
			ExtendedQueryOptimizerPipeline.ITERATIVE_EVALUATION_OPTIMIZER,
			ExtendedQueryOptimizerPipeline.FILTER_OPTIMIZER, // after join optimizer so we push down on the best statements
			StandardQueryOptimizerPipeline.ORDER_LIMIT_OPTIMIZER,
			joinAlgoOptimizer
		));
	}
}
