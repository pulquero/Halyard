package com.msd.gin.halyard.spin;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.StandardQueryOptimizerPipeline;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.memory.MemoryStoreConnection;

import com.msd.gin.halyard.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.algebra.evaluation.TupleFunctionEvaluationStrategy;
import com.msd.gin.halyard.sail.connection.SailConnectionQueryPreparer;

public class SpinMemoryStore extends MemoryStore {

	private final SpinParser spinParser = new SpinParser();
	private final FunctionRegistry functionRegistry = FunctionRegistry.getInstance();
	private final TupleFunctionRegistry tupleFunctionRegistry = TupleFunctionRegistry.getInstance();

	@Override
	public void init() {
		super.init();
		SpinFunctionInterpreter.registerSpinParsingFunctions(spinParser, functionRegistry, tupleFunctionRegistry);
		SpinMagicPropertyInterpreter.registerSpinParsingTupleFunctions(spinParser, tupleFunctionRegistry);
	}

	@Override
    protected NotifyingSailConnection getConnectionInternal() throws SailException {
        return new SpinMemoryStoreConnection(this);
    }

	final class SpinMemoryStoreConnection extends MemoryStoreConnection {
		private Boolean includeInferred;

		protected SpinMemoryStoreConnection(MemoryStore sail) {
			super(sail);
		}

		@Override
        protected EvaluationStrategy getEvaluationStrategy(Dataset dataset, final TripleSource tripleSource) {
			SailConnectionQueryPreparer queryPreparer = new SailConnectionQueryPreparer(this, includeInferred, getValueFactory());
			ExtendedTripleSourceWrapper extTripleSource = new ExtendedTripleSourceWrapper(tripleSource, queryPreparer);
			EvaluationStatistics stats = new EvaluationStatistics();
			TupleFunctionEvaluationStrategy evalStrat = new TupleFunctionEvaluationStrategy(extTripleSource, dataset, getFederatedServiceResolver(), tupleFunctionRegistry, 0, stats);
			evalStrat.setOptimizerPipeline(new StandardQueryOptimizerPipeline(evalStrat, extTripleSource, stats) {
				@Override
				public Iterable<QueryOptimizer> getOptimizers() {
					List<QueryOptimizer> optimizers = new ArrayList<>();
					optimizers.add(new SpinFunctionInterpreter(spinParser, extTripleSource, functionRegistry));
					optimizers.add(new SpinMagicPropertyInterpreter(spinParser, extTripleSource, tupleFunctionRegistry, null));
					for (QueryOptimizer optimizer : super.getOptimizers()) {
						optimizers.add(optimizer);
					}
					return optimizers;
				}
			});
			return evalStrat;
        }

		@Override
		protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(
				TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred)
			throws SailException
		{
			try {
				// as connections are not thread-safe we can store temporary state
				this.includeInferred = includeInferred;
				return super.evaluateInternal(tupleExpr, dataset, bindings, includeInferred);
			} finally {
				this.includeInferred = null;
			}
		}
	}


	static class ExtendedTripleSourceWrapper implements ExtendedTripleSource {
		private final TripleSource delegate;
		private QueryPreparer queryPreparer;

		ExtendedTripleSourceWrapper(TripleSource delegate, QueryPreparer queryPreparer) {
			this.delegate = delegate;
			this.queryPreparer = queryPreparer;
		}

		@Override
		public QueryPreparer getQueryPreparer() {
			return queryPreparer;
		}

		@Override
		public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
			return delegate.getStatements(subj, pred, obj, contexts);
		}

		@Override
		public ValueFactory getValueFactory() {
			return delegate.getValueFactory();
		}
	}
}
