package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.common.RDFRole;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.ValueConstraint;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.query.algebra.evaluation.PartitionableTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.function.ParallelSplitFunction;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.memory.MemoryStoreConnection;
import org.junit.Assert;

class MockSailWithConstraintsStrategy extends MemoryStore {
	static final String FORK_INDEX_BINDING = "forkIndex";

	@Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new MemoryStoreConnection(this) {
        	private int forkIndex = 0;
        	private int forkCount = 1;

        	@Override
        	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr,
        			Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
        		forkIndex = Literals.getIntValue(bindings.getValue(FORK_INDEX_BINDING), 0);
        		forkCount = ParallelSplitFunction.getNumberOfPartitionsFromFunctionArgument(tupleExpr, bindings);
        		return super.evaluateInternal(tupleExpr, dataset, bindings, includeInferred);
        	}

            @Override
            protected EvaluationStrategy getEvaluationStrategy(Dataset dataset, final TripleSource tripleSource) {
            	HalyardEvaluationStatistics stats = new HalyardEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY, null);
            	Configuration conf = new Configuration();
            	conf.setInt(StrategyConfig.HALYARD_EVALUATION_HASH_JOIN_LIMIT, 0);
            	conf.setFloat(StrategyConfig.HALYARD_EVALUATION_HASH_JOIN_COST_RATIO, Float.MAX_VALUE);
                HalyardEvaluationStrategy evalStrat = new HalyardEvaluationStrategy(conf, new MockTripleSource(tripleSource, forkIndex, forkCount), dataset, null, stats) {
                	@Override
                	public QueryEvaluationStep precompile(TupleExpr expr) {
                		return precompile(expr, new QueryEvaluationContext.Minimal(dataset, getValueFactory()));
                	}
                };
                evalStrat.setOptimizerPipeline(new HalyardQueryOptimizerPipeline(evalStrat, tripleSource.getValueFactory(), stats));
                return evalStrat;
            }

        };
    }


    static class MockTripleSource implements RDFStarTripleSource, PartitionableTripleSource {
        private final TripleSource tripleSource;
        private final int partitionIndex;
        private final int partitionCount;

        MockTripleSource(TripleSource tripleSource, int partitionIndex, int partitionCount) {
            this.tripleSource = tripleSource;
            this.partitionIndex = partitionIndex;
            this.partitionCount = partitionCount;
        }

        @Override
        public int getPartitionIndex() {
        	return partitionIndex;
        }

        @Override
        public int getPartitionCount() {
        	return partitionCount;
        }

        @Override
        public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
        	Assert.fail("Non-optimal strategy");
        	return null;
        }

        @Override
        public ValueFactory getValueFactory() {
            return tripleSource.getValueFactory();
        }

		@Override
		public CloseableIteration<? extends Triple, QueryEvaluationException> getRdfStarTriples(Resource subj, IRI pred, Value obj) throws QueryEvaluationException {
			return ((RDFStarTripleSource)tripleSource).getRdfStarTriples(subj, pred, obj);
		}

		@Override
		public TripleSource partition(StatementIndex.Name indexToUse, RDFRole.Name role, ValueConstraint constraint) {
			return new TripleSource() {
		        @Override
		        public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		            return new FilterIteration<Statement, QueryEvaluationException>(tripleSource.getStatements(subj, pred, obj, contexts)){
		                @Override
		                protected boolean accept(Statement stmt) throws QueryEvaluationException {
		                	Value v = role.getValue(stmt);
		                    return (partitionCount == 0 || Math.floorMod(v.hashCode(), partitionCount) == partitionIndex)
		                    	&& (constraint == null || constraint.test(v));
		                }
		            };
		        }

		        @Override
		        public ValueFactory getValueFactory() {
		            return tripleSource.getValueFactory();
		        }
			};
		}
    }
}
