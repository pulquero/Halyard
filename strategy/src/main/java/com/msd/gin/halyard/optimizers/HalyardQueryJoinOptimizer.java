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

import com.msd.gin.halyard.query.algebra.SkipVarsQueryModelVisitor;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.IncompatibleOperationException;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
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
        final Set<String> parallelSplitBindings = getParallelSplitBindings(tupleExpr);
        tupleExpr.visit(new QueryJoinOptimizer.JoinVisitor() {
			@Override
			protected void updateCardinalityMap(TupleExpr tupleExpr, Map<TupleExpr,Double> cardinalityMap) {
				statistics.updateCardinalityMap(tupleExpr, boundVars, parallelSplitBindings, cardinalityMap, true);
			}
		});
	}

	private Set<String> getParallelSplitBindings(TupleExpr tupleExpr) {
        final Set<String> parallelSplitBindings = new HashSet<>();
        tupleExpr.visit(new SkipVarsQueryModelVisitor<RuntimeException>() {
            @Override
            public void meet(FunctionCall node) throws RuntimeException {
                if (HALYARD.PARALLEL_SPLIT_FUNCTION.stringValue().equals(node.getURI())) {
                    for (ValueExpr arg : node.getArgs()) {
                        if (arg instanceof Var) {
                        	parallelSplitBindings.add(((Var)arg).getName());
                        }
                    }
                }
                super.meet(node);
            }
        });
        return parallelSplitBindings;
	}
}
