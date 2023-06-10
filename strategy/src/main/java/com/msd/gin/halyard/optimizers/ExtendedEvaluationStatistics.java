package com.msd.gin.halyard.optimizers;

import com.google.common.collect.Iterables;
import com.msd.gin.halyard.algebra.StarJoin;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.Slice;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TripleRef;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

public class ExtendedEvaluationStatistics extends EvaluationStatistics {
    /** heuristic - assume 80/20 rule */
	public static final double COMPLETENESS_FACTOR = 0.8;

	protected final StatementPatternCardinalityCalculator.Factory spcalcFactory;
	protected final ServiceStatisticsProvider srvStatsProvider;

	public ExtendedEvaluationStatistics(@Nonnull StatementPatternCardinalityCalculator.Factory spcalcFactory) {
		this(spcalcFactory, null);
	}

	public ExtendedEvaluationStatistics(@Nonnull StatementPatternCardinalityCalculator.Factory spcalcFactory, @Nullable ServiceStatisticsProvider srvStatsProvider) {
		this.spcalcFactory = spcalcFactory;
		this.srvStatsProvider = srvStatsProvider;
	}

	protected final ExtendedEvaluationStatistics getStatisticsFor(TupleExpr expr) {
		QueryModelNode parent = expr.getParentNode();
		while(parent != null && !(parent instanceof Service)) {
			parent = parent.getParentNode();
		}
		Service service = (Service) parent;
		if (service == null) {
			return this;
		}
		if (srvStatsProvider != null) {
			IRI serviceUrl = (IRI) service.getServiceRef().getValue();
			if (serviceUrl != null) {
				return srvStatsProvider.getStatisticsForService(serviceUrl.stringValue());
			}
		}
		return new ExtendedEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY);
	}

	public void updateCardinalityMap(TupleExpr expr, Set<String> boundVars, Map<TupleExpr, Double> mapToUpdate) {
		getStatisticsFor(expr).updateCardinalityMapInternal(expr, boundVars, mapToUpdate);
	}

	protected final void updateCardinalityMapInternal(TupleExpr expr, Set<String> boundVars, Map<TupleExpr, Double> mapToUpdate) {
		try (StatementPatternCardinalityCalculator spcalc = spcalcFactory.create()) {
			ExtendedCardinalityCalculator cc = new ExtendedCardinalityCalculator(spcalc, srvStatsProvider, boundVars, mapToUpdate);
			expr.visit(cc);
		} catch(IOException ioe) {
			throw new QueryEvaluationException(ioe);
		}
	}

	public double getCardinality(TupleExpr expr, Set<String> boundVariables) {
		return getStatisticsFor(expr).getCardinalityInternal(expr, boundVariables);
	}

	protected final double getCardinalityInternal(TupleExpr expr, Set<String> boundVariables) {
		try (StatementPatternCardinalityCalculator spcalc = spcalcFactory.create()) {
			ExtendedCardinalityCalculator cc = new ExtendedCardinalityCalculator(spcalc, srvStatsProvider, boundVariables, null);
			expr.visit(cc);
			return cc.getCardinality();
		} catch(IOException ioe) {
			throw new QueryEvaluationException(ioe);
		}
	}

	@Override
	public final double getCardinality(TupleExpr expr) {
		return getCardinality(expr, Collections.emptySet());
	}

	@Override
	protected final CardinalityCalculator createCardinalityCalculator() {
		// should never be called
		throw new AssertionError();
	}


	protected static class ExtendedCardinalityCalculator extends CardinalityCalculator {

		protected static final double VAR_CARDINALITY = 10.0;
		private static final double TFC_COST_FACTOR = 0.1;

		protected final StatementPatternCardinalityCalculator spcalc;
		protected final ServiceStatisticsProvider srvStatsProvider;
		protected final Set<String> boundVars;
        protected final Map<TupleExpr, Double> mapToUpdate;

		public ExtendedCardinalityCalculator(@Nonnull StatementPatternCardinalityCalculator spcalc, @Nullable ServiceStatisticsProvider srvStatsProvider, Set<String> boundVariables, @Nullable Map<TupleExpr, Double> mapToUpdate) {
			this.spcalc = spcalc;
			this.srvStatsProvider = srvStatsProvider;
			this.boundVars = boundVariables;
            this.mapToUpdate = mapToUpdate;
		}

		protected ExtendedCardinalityCalculator newCardinalityCalculator(Set<String> newBoundVars) {
			return new ExtendedCardinalityCalculator(spcalc, srvStatsProvider, newBoundVars, mapToUpdate);
		}

		private boolean isCardinalityCached(AbstractQueryModelNode expr) {
			double estimate = expr.getResultSizeEstimate();
        	if (estimate >= 0.0) {
        		cardinality = estimate;
        		return true;
        	} else {
        		return false;
        	}
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
		public void meet(BindingSetAssignment node) {
			cardinality = getCardinality(node);
			updateMap(node);
		}

		@Override
		public void meet(ArbitraryLengthPath node) {
			final Var pathVar = new Var("_anon_path", true);
			// cardinality of ALP is determined based on the cost of a
			// single ?s ?p ?o ?c pattern where ?p is unbound, compensating for the fact that
			// the length of the path is unknown but expected to be _at least_ twice that of a normal
			// statement pattern.
			cardinality = 2.0 * getCardinality(new StatementPattern(node.getSubjectVar().clone(), pathVar,
					node.getObjectVar().clone(), node.getContextVar() != null ? node.getContextVar().clone() : null));
			updateMap(node);
		}

		@Override
        public void meet(ZeroLengthPath node) {
            super.meet(node);
            updateMap(node);
        }

		@Override
        public void meet(Slice node) {
            super.meet(node);
            if (node.hasLimit()) {
            	cardinality = node.getLimit();
            }
            updateMap(node);
        }

		@Override
		public void meet(StatementPattern sp) {
        	if (!isCardinalityCached(sp)) {
        		cardinality = getCardinality(sp);
        	}
			updateMap(sp);
		}

		@Override
		public void meet(TripleRef tripleRef) {
        	if (!isCardinalityCached(tripleRef)) {
        		cardinality = getCardinality(tripleRef);
        	}
			updateMap(tripleRef);
		}

		@Override
		protected double getCardinality(BindingSetAssignment bsa) {
			return Iterables.size(bsa.getBindingSets());
		}

		@Override
		protected double getCardinality(StatementPattern sp) {
			return spcalc.getStatementCardinality(sp.getSubjectVar(), sp.getPredicateVar(), sp.getObjectVar(), sp.getContextVar(), boundVars);
		}

		@Override
		protected double getCardinality(TripleRef tripleRef) {
			if (boundVars.contains(tripleRef.getExprVar().getName())) {
				return 1.0;
			} else {
				return spcalc.getTripleCardinality(tripleRef.getSubjectVar(), tripleRef.getPredicateVar(), tripleRef.getObjectVar(), boundVars);
			}
		}

		@Override
		protected final double getCardinality(double varCardinality, Var var) {
			return SimpleStatementPatternCardinalityCalculator.getCardinality(var, boundVars, varCardinality);
		}

		@Override
		protected final int countConstantVars(Iterable<Var> vars) {
			int constantVarCount = 0;
			for(Var var : vars) {
				if(SimpleStatementPatternCardinalityCalculator.hasValue(var, boundVars)) {
					constantVarCount++;
				}
			}
			return constantVarCount;
		}

        @Override
        public void meet(Filter node) {
            node.getArg().visit(this);
            cardinality *= COMPLETENESS_FACTOR;
            updateMap(node);
        }

        @Override
        public void meet(Join node) {
            meetJoin(node);
            updateMap(node);
        }

        @Override
        public void meet(LeftJoin node) {
            meetJoin(node);
            cardinality *= COMPLETENESS_FACTOR;
            updateMap(node);
        }

        protected void meetJoin(BinaryTupleOperator node) {
            meetJoinLeft(node.getLeftArg());
            double leftArgCost = this.cardinality;

            Set<String> newBoundVars = new HashSet<>(boundVars);
            newBoundVars.addAll(node.getLeftArg().getBindingNames());
            meetJoinRight(node.getRightArg(), newBoundVars);
            cardinality *= leftArgCost;
        }

        protected void meetJoinLeft(TupleExpr left) {
        	left.visit(this);
        	updateMap(left);
        }

        protected void meetJoinRight(TupleExpr right, Set<String> newBoundVars) {
            ExtendedCardinalityCalculator newCalc = newCardinalityCalculator(newBoundVars);
        	right.visit(newCalc);
            cardinality = newCalc.cardinality;
            updateMap(right);
        }

        @Override
        protected void meetUnaryTupleOperator(UnaryTupleOperator node) {
            super.meetUnaryTupleOperator(node);
            updateMap(node);
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
    	public void meetOther(QueryModelNode node) {
    		if (node instanceof TupleFunctionCall) {
    			meet((TupleFunctionCall)node);
    		} else if (node instanceof StarJoin) {
    			meet((StarJoin)node);
    		} else {
    			super.meetOther(node);
    		}
    	}

        public void meet(StarJoin node) {
        	double card = Double.POSITIVE_INFINITY;
        	for (TupleExpr sp : node.getArgs()) {
        		sp.visit(this);
        		card = Math.min(card, this.cardinality);
        	}
        	Set<Var> vars = new HashSet<>();
        	node.getVars(vars);
        	vars.remove(node.getCommonVar());
        	int constCount = countConstantVars(vars);
            cardinality = card*Math.pow(VAR_CARDINALITY*VAR_CARDINALITY, (double)(vars.size()-constCount)/vars.size());
            updateMap(node);
        }

        public void meet(TupleFunctionCall node) {
			// must have all arguments bound to be able to evaluate
			double argCard = 1.0;
			for (ValueExpr expr : node.getArgs()) {
				if (expr instanceof Var) {
					argCard *= getCardinality(VAR_CARDINALITY, (Var) expr);
				} else if (expr instanceof ValueConstant) {
					argCard *= 1;
				} else {
					argCard *= VAR_CARDINALITY;
				}
			}
			// output cardinality tends to be independent of number of result vars
			cardinality = TFC_COST_FACTOR * argCard * VAR_CARDINALITY;
			updateMap(node);
		}

        @Override
        public void meet(Service node) {
            ExtendedEvaluationStatistics srvStats = null;
    		if (srvStatsProvider != null) {
    			IRI serviceUrl = (IRI) node.getServiceRef().getValue();
    			if (serviceUrl != null) {
    				srvStats = srvStatsProvider.getStatisticsForService(serviceUrl.stringValue());
    			}
    		}
            if (srvStats != null) {
                TupleExpr remoteExpr = node.getServiceExpr();
                meetServiceExpr(remoteExpr, srvStats);
            } else {
                super.meet(node);
            }
            updateMap(node);
        }

        protected void meetServiceExpr(TupleExpr remoteExpr, ExtendedEvaluationStatistics srvStats) {
        	if (mapToUpdate != null) {
        		srvStats.updateCardinalityMapInternal(remoteExpr, boundVars, mapToUpdate);
        		cardinality = mapToUpdate.get(remoteExpr);
        	} else {
        		cardinality = srvStats.getCardinalityInternal(remoteExpr, boundVars);
        	}
        }

        protected void updateMap(TupleExpr node) {
            if (mapToUpdate != null) {
                mapToUpdate.put(node, cardinality);
            }
        }
	}
}
