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

import com.msd.gin.halyard.algebra.StarJoin;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TripleRef;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;

/**
 * Must be thread-safe.
 * @author Adam Sotona (MSD)
 */
public final class HalyardEvaluationStatistics extends ExtendedEvaluationStatistics {
	static final double PRIORITY_VAR_FACTOR = 1000000.0;

    public HalyardEvaluationStatistics(@Nonnull StatementPatternCardinalityCalculator.Factory spcalcFactory, @Nullable ServiceStatisticsProvider srvStatsProvider) {
        super(spcalcFactory, srvStatsProvider);
    }

	public void updateCardinalityMap(TupleExpr expr, Set<String> boundVars, Set<String> priorityVars, Map<TupleExpr, Double> mapToUpdate) {
		ExtendedEvaluationStatistics stats = getStatisticsFor(expr);
		if (stats instanceof HalyardEvaluationStatistics) {
			((HalyardEvaluationStatistics)stats).updateCardinalityMapInternal(expr, boundVars, priorityVars, mapToUpdate);
		}
	}

	private void updateCardinalityMapInternal(TupleExpr expr, Set<String> boundVars, Set<String> priorityVars, Map<TupleExpr, Double> mapToUpdate) {
		try (StatementPatternCardinalityCalculator spcalc = spcalcFactory.create()) {
			HalyardCardinalityCalculator cc = new HalyardCardinalityCalculator(spcalc, srvStatsProvider, boundVars, priorityVars, mapToUpdate);
			expr.visit(cc);
		} catch(IOException ioe) {
			throw new QueryEvaluationException(ioe);
		}
	}

	public double getCardinality(TupleExpr expr, Set<String> boundVariables, Set<String> priorityVariables) {
		ExtendedEvaluationStatistics stats = getStatisticsFor(expr);
		if (stats instanceof HalyardEvaluationStatistics) {
			return ((HalyardEvaluationStatistics)stats).getCardinalityInternal(expr, boundVariables, priorityVariables);
		} else {
			return stats.getCardinalityInternal(expr, boundVariables);
		}
	}

	private double getCardinalityInternal(TupleExpr expr, Set<String> boundVariables, Set<String> priorityVariables) {
		try (StatementPatternCardinalityCalculator spcalc = spcalcFactory.create()) {
			HalyardCardinalityCalculator cc = new HalyardCardinalityCalculator(spcalc, srvStatsProvider, boundVariables, priorityVariables, null);
			expr.visit(cc);
			return cc.getCardinality();
		} catch(IOException ioe) {
			throw new QueryEvaluationException(ioe);
		}
	}

	@Override
	public double getCardinality(TupleExpr expr, Set<String> boundVariables) {
		return getCardinality(expr, boundVariables, Collections.emptySet());
	}


    private static class HalyardCardinalityCalculator extends ExtendedCardinalityCalculator {

        private final Set<String> priorityVariables;
        private final Map<TupleExpr, Double> mapToUpdate;

        HalyardCardinalityCalculator(@Nonnull StatementPatternCardinalityCalculator spcalc, ServiceStatisticsProvider srvStatsProvider, Set<String> boundVariables, Set<String> priorityVariables, @Nullable Map<TupleExpr, Double> mapToUpdate) {
        	super(spcalc, srvStatsProvider, boundVariables);
            this.priorityVariables = priorityVariables;
            this.mapToUpdate = mapToUpdate;
        }

        @Override
        protected double getCardinality(StatementPattern sp) {
            //always prefer HALYARD.SEARCH_TYPE object literals to move such statements higher in the joins tree
            Var objectVar = sp.getObjectVar();
            if (HalyardEvaluationStrategy.isSearchStatement(objectVar.getValue())) {
                return 0.0001;
            }
            double card = super.getCardinality(sp);
            for (Var v : sp.getVarList()) {
                //decrease cardinality for each priority variable present
                if (v != null && priorityVariables.contains(v.getName())) {
                	card /= PRIORITY_VAR_FACTOR;
                }
            }
            return card;
        }

        @Override
        protected double getCardinality(TripleRef tripleRef) {
            double card = super.getCardinality(tripleRef);
            for (Var v : tripleRef.getVarList()) {
                //decrease cardinality for each priority variable present
                if (v != null && priorityVariables.contains(v.getName())) {
                	card /= PRIORITY_VAR_FACTOR;
                }
            }
            return card;
        }

        @Override
        protected void meetJoin(BinaryTupleOperator node) {
        	super.meetJoin(node);
            updateMap(node);
        }

        @Override
        protected void meetJoinLeft(TupleExpr left) {
        	super.meetJoinLeft(left);
        	updateMap(left);
        }

        @Override
        protected void meetJoinRight(TupleExpr right, Set<String> newBoundVars) {
            HalyardCardinalityCalculator newCalc = new HalyardCardinalityCalculator(spcalc, srvStatsProvider, newBoundVars, priorityVariables, mapToUpdate);
            right.visit(newCalc);
            cardinality = newCalc.cardinality;
            updateMap(right);
        }

        @Override
        protected void meetBinaryTupleOperator(BinaryTupleOperator node) {
            node.getLeftArg().visit(this);
            updateMap(node.getLeftArg());
            double leftArgCost = this.cardinality;
            node.getRightArg().visit(this);
            updateMap(node.getRightArg());
            cardinality += leftArgCost;
            updateMap(node);
        }

        @Override
        public void meet(Filter node) {
        	super.meet(node);
            updateMap(node);
        }

        @Override
        protected void meetUnaryTupleOperator(UnaryTupleOperator node) {
            super.meetUnaryTupleOperator(node);
            updateMap(node);
        }

        @Override
        public void meet(StatementPattern sp) {
            super.meet(sp);
            updateMap(sp);
        }

        @Override
        public void meet(TripleRef node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(ArbitraryLengthPath node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(ZeroLengthPath node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(BindingSetAssignment node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(EmptySet node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(SingletonSet node) {
            super.meet(node);
            updateMap(node);
        }

        @Override
        public void meet(StarJoin node) {
        	super.meet(node);
        	updateMap(node);
        }

        @Override
        public void meet(TupleFunctionCall node) {
        	super.meet(node);
			updateMap(node);
		}

        @Override
        public void meet(Service node) {
        	super.meet(node);
            updateMap(node);
        }

        @Override
        protected void meetServiceExpr(TupleExpr remoteExpr, ExtendedEvaluationStatistics srvStats) {
        	if (srvStats instanceof HalyardEvaluationStatistics) {
	            if (mapToUpdate != null) {
	                ((HalyardEvaluationStatistics)srvStats).updateCardinalityMapInternal(remoteExpr, boundVars, priorityVariables, mapToUpdate);
	                cardinality = mapToUpdate.get(remoteExpr);
	            } else {
	            	cardinality = ((HalyardEvaluationStatistics)srvStats).getCardinalityInternal(remoteExpr, boundVars, priorityVariables);
	            }
        	} else {
        		super.meetServiceExpr(remoteExpr, srvStats);
        	}
        }

        private void updateMap(TupleExpr node) {
            if (mapToUpdate != null) {
                mapToUpdate.put(node, cardinality);
            }
        }
    }
}
