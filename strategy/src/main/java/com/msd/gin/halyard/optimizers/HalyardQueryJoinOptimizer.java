/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.ConstrainedStatementPattern;
import com.msd.gin.halyard.query.algebra.SkipVarsQueryModelVisitor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardQueryJoinOptimizer extends QueryJoinOptimizer {
	private final HalyardEvaluationStatistics statistics;

	public HalyardQueryJoinOptimizer(HalyardEvaluationStatistics statistics) {
        super(statistics);
        this.statistics = statistics;
    }

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		// Halyard - find bindings required for parallel split function
        tupleExpr.visit(new QueryJoinOptimizer.JoinVisitor() {
        	Set<String> parallelSplitBindings = null;

        	private Set<String> getParallelSplitBindings() {
        		if (parallelSplitBindings == null) {
            		parallelSplitBindings = findParallelSplitBindings(tupleExpr);
        		}
        		return parallelSplitBindings;
        	}

        	@Override
        	public void meet(Projection node) {
        		Set<String> currentParallelSplitBindings = parallelSplitBindings;
        		if (node.isSubquery()) {
        			parallelSplitBindings = findParallelSplitBindings(node.getArg());
        		}
        		super.meet(node);
        		parallelSplitBindings = currentParallelSplitBindings;
        	}

        	@Override
			protected void updateCardinalityMap(TupleExpr node, Map<TupleExpr,Double> cardinalityMap) {
				statistics.updateCardinalityMap(node, boundVars, getParallelSplitBindings(), cardinalityMap, true);
			}
		});
	}

	private static Set<String> findParallelSplitBindings(TupleExpr tupleExpr) {
        final Set<String> parallelSplitBindings = new HashSet<>();
        tupleExpr.visit(new SkipVarsQueryModelVisitor<RuntimeException>() {
        	/**
        	 * Filters that could not be optimised.
        	 */
            @Override
            public void meet(FunctionCall node) {
                if (HALYARD.PARALLEL_SPLIT_FUNCTION.stringValue().equals(node.getURI())) {
                    for (ValueExpr arg : node.getArgs()) {
                        if (arg instanceof Var) {
                            parallelSplitBindings.add(((Var)arg).getName());
                        }
                    }
                }
                super.meet(node);
            }

        	/**
        	 * Filters that could be optimised.
        	 */
            @Override
            public void meet(StatementPattern node) {
                if (node instanceof ConstrainedStatementPattern) {
                    ConstrainedStatementPattern csp = (ConstrainedStatementPattern) node;
                    if (csp.getConstraint().isPartitioned()) {
                       	parallelSplitBindings.add(csp.getConstrainedVar().getName());
                    }
                }
                super.meet(node);
            }

            @Override
        	public void meet(Projection node) {
            	// respect variable scoping
        		if (!node.isSubquery()) {
        			super.meet(node);
        		}
        	}
        });
        return parallelSplitBindings;
	}
}
