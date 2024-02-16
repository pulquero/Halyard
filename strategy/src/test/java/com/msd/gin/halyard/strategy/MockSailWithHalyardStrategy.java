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

import com.msd.gin.halyard.federation.SailFederatedService;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.JoinAlgorithmOptimizer;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.BindingSetPipeQueryEvaluationStep;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.QueryPreparer;

import java.util.LinkedList;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.IterationWrapper;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.memory.MemoryStoreConnection;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class MockSailWithHalyardStrategy extends MemoryStore {

	private final LinkedList<TupleExpr> queryHistory = new LinkedList<>();
	private final int optHashJoinLimit;
	private final int evalHashJoinLimit;
	private final float cardinalityRatio;
	private final int minJoins;
	private final int minUnions;
	private final int pullAllLimit;

	MockSailWithHalyardStrategy() {
		this(0, 0, Float.MAX_VALUE, 1, 1, 0);
		SPARQLServiceResolver fsr = new SPARQLServiceResolver();
		fsr.registerService("repository:memory", new SailFederatedService(new MemoryStore()));
		fsr.registerService("repository:pushOnly", new SailFederatedService(new PushOnlyMockSail()));
		fsr.registerService("repository:askOnly", new SailFederatedService(new MemoryStore()) {
			@Override
			public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void select(Consumer<BindingSet> handler, Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void select(BindingSetPipe pipe, Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
				throw new UnsupportedOperationException();
			}

			@Override
			public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service, CloseableIteration<BindingSet, QueryEvaluationException> bindings, String baseUri) throws QueryEvaluationException {
				throw new UnsupportedOperationException();
			}
		});
		setFederatedServiceResolver(fsr);
	}

	MockSailWithHalyardStrategy(int optHashJoinLimit, int evalHashJoinLimit, float cardinalityRatio, int starJoinMin, int naryUnionMin, int pullAllLimit) {
		this.optHashJoinLimit = optHashJoinLimit;
		this.evalHashJoinLimit = evalHashJoinLimit;
		this.cardinalityRatio = cardinalityRatio;
		this.minJoins = starJoinMin;
		this.minUnions = naryUnionMin;
		this.pullAllLimit = pullAllLimit;
	}

	LinkedList<TupleExpr> getQueryHistory() {
		return queryHistory;
	}

	@Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new MemoryStoreConnectionWithHalyardStrategy(this);
    }

	final class MemoryStoreConnectionWithHalyardStrategy extends MemoryStoreConnection {
		protected MemoryStoreConnectionWithHalyardStrategy(MemoryStore sail) {
			super(sail);
		}

		@Override
        protected EvaluationStrategy getEvaluationStrategy(Dataset dataset, final TripleSource tripleSource) {
        	HalyardEvaluationStatistics stats = new HalyardEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY, null);
        	Configuration conf = new Configuration();
        	conf.setInt(StrategyConfig.HALYARD_EVALUATION_HASH_JOIN_LIMIT, optHashJoinLimit);
        	conf.setFloat(StrategyConfig.HALYARD_EVALUATION_HASH_JOIN_COST_RATIO, cardinalityRatio);
        	conf.setInt(StrategyConfig.HALYARD_EVALUATION_STAR_JOIN_MIN_JOINS, minJoins);
        	conf.setInt(StrategyConfig.HALYARD_EVALUATION_NARY_UNION_MIN_UNIONS, minUnions);
        	conf.setInt(StrategyConfig.HALYARD_EVALUATION_PULL_PUSH_ASYNC_ALL_LIMIT, pullAllLimit);
        	HalyardEvaluationStrategy evalStrat = new HalyardEvaluationStrategy(conf, new MockTripleSource(tripleSource), dataset, getFederatedServiceResolver(), stats) {
        		@Override
        		public BindingSetPipeQueryEvaluationStep precompile(TupleExpr expr, QueryEvaluationContext evalContext) {
        			queryHistory.add(expr);
        			return super.precompile(expr, evalContext);
        		}
        		@Override
        		protected JoinAlgorithmOptimizer getJoinAlgorithmOptimizer() {
        			return new JoinAlgorithmOptimizer(stats, evalHashJoinLimit, cardinalityRatio);
        		}
        	};
            evalStrat.setOptimizerPipeline(new HalyardQueryOptimizerPipeline(evalStrat, tripleSource.getValueFactory(), stats));
            evalStrat.setTrackResultSize(true);
            evalStrat.setTrackTime(true);
            return evalStrat;
        }
	}

    static class MockTripleSource implements RDFStarTripleSource, ExtendedTripleSource {
        private final TripleSource tripleSource;

        MockTripleSource(TripleSource tripleSource) {
            this.tripleSource = tripleSource;
        }

        @Override
        public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
            return new IterationWrapper<Statement, QueryEvaluationException>(tripleSource.getStatements(subj, pred, obj, contexts)){
            	final long CONNECTION_DELAY = 20L;
            	final long NEXT_DELAY = 2L;
                long delay = CONNECTION_DELAY;

                @Override
                public boolean hasNext() throws QueryEvaluationException {
                    // emulate time taken to perform a HBase scan.
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ex) {
                        //ignore
                    }
                    delay = NEXT_DELAY;
                    return super.hasNext();
                }
            };
        }

        @Override
        public ValueFactory getValueFactory() {
            return tripleSource.getValueFactory();
        }

		@Override
		public CloseableIteration<? extends Triple, QueryEvaluationException> getRdfStarTriples(Resource subj, IRI pred, Value obj) throws QueryEvaluationException {
			return ((RDFStarTripleSource)tripleSource).getRdfStarTriples(subj, pred, obj);
		}

		public QueryPreparer newQueryPreparer() {
			throw new AssertionError();
		}
    }
}
