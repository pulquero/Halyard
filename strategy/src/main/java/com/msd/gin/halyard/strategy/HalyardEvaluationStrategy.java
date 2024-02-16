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

import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.ModelTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.function.ParallelSplitFunction;
import com.msd.gin.halyard.federation.HalyardFederatedService;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.JoinAlgorithmOptimizer;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.BindingSetPipeQueryEvaluationStep;
import com.msd.gin.halyard.query.ValuePipe;
import com.msd.gin.halyard.query.ValuePipeQueryValueEvaluationStep;
import com.msd.gin.halyard.strategy.HalyardTupleExprEvaluation.QuadPattern;
import com.msd.gin.halyard.strategy.HalyardValueExprEvaluation.ConvertingValuePipe;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.IterationWrapper;
import org.eclipse.rdf4j.common.transaction.QueryEvaluationMode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryValueEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtility;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.CustomAggregateFunctionRegistry;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

/**
 * Provides an efficient asynchronous parallel push {@code EvaluationStrategy} implementation for query evaluation in Halyard. This is the default strategy
 * in Halyard. An alternative strategy is the {@code StrictEvaluationStrategy} from RDF4J.
 * @author Adam Sotona (MSD)
 */
public class HalyardEvaluationStrategy implements EvaluationStrategy {
	private final StrategyConfig config;
	/**
	 * Used to allow queries across more than one Halyard datasets
	 */
    private final FederatedServiceResolver serviceResolver;
    private final Map<String,FederatedService> federatedServices = new ConcurrentHashMap<>();
    private final TripleSource tripleSource;
    private final Dataset dataset;
    private final HalyardEvaluationExecutor executor;
    /**
     * Evaluates TupleExpressions and all implementations of that interface
     */
    private final HalyardTupleExprEvaluation tupleEval;

    /**
     * Evaluates ValueExpr expressions and all implementations of that interface
     */
    private final HalyardValueExprEvaluation valueEval;

    private final boolean isStrict = false;

    /** Track the results size that each node in the query plan produces during execution. */
	private boolean trackResultSize;

	/** Track the exeution time of each node in the plan. */
	private boolean trackResultTime;

	private boolean trackBranchOperatorsOnly;

	private QueryOptimizerPipeline pipeline;

	final FunctionRegistry functionRegistry;
	final CustomAggregateFunctionRegistry aggregateFunctionRegistry;
	final TupleFunctionRegistry tupleFunctionRegistry;

	/**
	 * Default constructor of HalyardEvaluationStrategy
	 * 
	 * @param config configuration
	 * @param tripleSource {@code TripleSource} to be queried for the existence of triples in a context
	 * @param tupleFunctionRegistry {@code TupleFunctionRegistry} to use for {@code TupleFunctionCall} evaluation.
	 * @param functionRegistry {@code FunctionRegistry} to use for {@code FunctionCall} evaluation.
	 * @param aggregateFunctionRegistry {@code CustomAggregateFunctionRegistry} to use for {@code AggregateFunctionCall} evaluation.
	 * @param dataset {@code Dataset} A dataset consists of a default graph for read and using operations, which is the RDF merge of one or more graphs, a set of named graphs, and
	 * a single update graph for INSERT and DELETE
	 * @param serviceResolver {@code FederatedServiceResolver} resolver for any federated services (graphs) required for the evaluation
	 * @param statistics statistics to use
	 * @param executor executor to use
	 */
	public HalyardEvaluationStrategy(StrategyConfig config, TripleSource tripleSource,
			TupleFunctionRegistry tupleFunctionRegistry,
			FunctionRegistry functionRegistry,
			CustomAggregateFunctionRegistry aggregateFunctionRegistry,
			Dataset dataset, FederatedServiceResolver serviceResolver,
			HalyardEvaluationStatistics statistics, HalyardEvaluationExecutor executor) {
		this.config = config;
		this.tripleSource = tripleSource;
		this.dataset = dataset;
		this.serviceResolver = serviceResolver;
		this.executor = executor;
		this.functionRegistry = functionRegistry;
		this.aggregateFunctionRegistry = aggregateFunctionRegistry;
		this.tupleFunctionRegistry = tupleFunctionRegistry;
		this.tupleEval = new HalyardTupleExprEvaluation(this, tripleSource, dataset, executor);
		this.valueEval = new HalyardValueExprEvaluation(this, tripleSource, executor.getQueuePollTimeoutMillis());
		this.pipeline = new HalyardQueryOptimizerPipeline(this, tripleSource.getValueFactory(), statistics);
	}

	HalyardEvaluationStrategy(Configuration conf, TripleSource tripleSource, Dataset dataset,
			FederatedServiceResolver serviceResolver, HalyardEvaluationStatistics statistics) {
		this(new StrategyConfig(conf), tripleSource,
			TupleFunctionRegistry.getInstance(),
			FunctionRegistry.getInstance(),
			CustomAggregateFunctionRegistry.getInstance(),
			dataset, serviceResolver, statistics, new HalyardEvaluationExecutor(conf));
	}

	@Override
	public void setQueryEvaluationMode(QueryEvaluationMode mode) {
		// always STANDARD
	}

	@Override
	public QueryEvaluationMode getQueryEvaluationMode() {
		return QueryEvaluationMode.STANDARD;
	}

	@Override
	public void setTrackResultSize(boolean trackResultSize) {
		this.trackResultSize = trackResultSize;
	}

	@Override
	public boolean isTrackResultSize() {
		return trackResultSize;
	}

	@Override
	public void setTrackTime(boolean trackTime) {
		this.trackResultTime = trackTime;
	}

	public boolean isTrackTime() {
		return trackResultTime;
	}

	public boolean isTrackBranchOperatorsOnly() {
		return trackBranchOperatorsOnly;
	}

	public void setTrackBranchOperatorsOnly(boolean f) {
		trackBranchOperatorsOnly = f;
	}

	boolean isStrict() {
		return isStrict;
	}

	StrategyConfig getConfig() {
		return config;
	}

	TripleSource getTripleSource() {
		return tripleSource;
	}

	public HalyardEvaluationExecutor getExecutor() {
		return executor;
	}

	protected JoinAlgorithmOptimizer getJoinAlgorithmOptimizer() {
    	if (pipeline instanceof HalyardQueryOptimizerPipeline) {
    		return ((HalyardQueryOptimizerPipeline)pipeline).getJoinAlgorithmOptimizer();
    	} else {
    		return null;
    	}
	}

	/**
     * Get a service for a federated dataset.
     */
    @Override
    public FederatedService getService(String serviceUrl) throws QueryEvaluationException {
    	return getService(serviceUrl, 0, 1);
    }

    FederatedService getService(String serviceUrl, int forkIndex, int forkCount) throws QueryEvaluationException {
        if (serviceResolver == null) {
            throw new QueryEvaluationException("No Service Resolver set.");
        }
        return federatedServices.computeIfAbsent(serviceUrl, (endpoint) -> {
        	FederatedService fedService = serviceResolver.getService(serviceUrl);
        	if (fedService instanceof HalyardFederatedService) {
        		fedService = ((HalyardFederatedService)fedService).createEvaluationInstance(this, forkIndex, forkCount);
        	}
        	return fedService;
        });
    }

	@Override
	public void setOptimizerPipeline(QueryOptimizerPipeline pipeline) {
		Objects.requireNonNull(pipeline);
		this.pipeline = pipeline;
	}

	@Override
	public TupleExpr optimize(TupleExpr expr, EvaluationStatistics evaluationStatistics, BindingSet bindings) {
		TupleExpr optimizedExpr = expr;
		for (QueryOptimizer optimizer : pipeline.getOptimizers()) {
			optimizer.optimize(optimizedExpr, dataset, bindings);
		}
		return optimizedExpr;
	}

    /**
     * Called by RDF4J to evaluate a query or part of a query using a service
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service, String serviceUri, CloseableIteration<BindingSet, QueryEvaluationException> bindings) throws QueryEvaluationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BindingSetPipeQueryEvaluationStep precompile(TupleExpr expr, QueryEvaluationContext context) {
    	return tupleEval.precompile(expr, context);
    }

    @Override
    public QueryEvaluationStep precompile(TupleExpr expr) {
    	return precompile(expr, new QueryEvaluationContext.Minimal(dataset, tripleSource.getValueFactory()));
    }

    /**
	 * Called by RDF4J to evaluate a tuple expression
	 */
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
		return precompile(expr).evaluate(bindings);
	}

	CloseableIteration<BindingSet, QueryEvaluationException> track(CloseableIteration<BindingSet, QueryEvaluationException> iter, TupleExpr expr) {
		if (!trackBranchOperatorsOnly || Algebra.isBranchTupleOperator(expr) || (expr instanceof QueryRoot)) {
			if (trackResultTime) {
				iter = new TimedIterator(iter, expr);
			}
		
			if (trackResultSize) {
				iter = new ResultSizeCountingIterator(iter, expr);
			}
		}

		return iter;
	}

	BindingSetPipe track(BindingSetPipe parent, TupleExpr expr) {
		if (!trackBranchOperatorsOnly || Algebra.isBranchTupleOperator(expr) || (expr instanceof QueryRoot)) {
			if (trackResultTime) {
				parent = new TimedBindingSetPipe(parent, expr);
			}
	
			if (trackResultSize) {
				parent = new ResultSizeCountingBindingSetPipe(parent, expr);
			}
		}

		return parent;
	}

    @Override
    public ValuePipeQueryValueEvaluationStep precompile(ValueExpr expr, QueryEvaluationContext context) {
    	return valueEval.precompile(expr, context);
    }

	/**
     * Called by RDF4J to evaluate a value expression.
     */
    @Override
    public Value evaluate(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        return valueEval.precompile(expr, new QueryEvaluationContext.Minimal(dataset, tripleSource.getValueFactory())).evaluate(bindings);
    }

    /**
     * Called by RDF4J to evaluate a boolean expression.
     */
    @Override
    public boolean isTrue(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
    	return isTrue(valueEval.precompile(expr, new QueryEvaluationContext.Minimal(dataset, tripleSource.getValueFactory())), bindings);
    }

	@Override
	public boolean isTrue(QueryValueEvaluationStep step, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
		Value value = step.evaluate(bindings);
		return QueryEvaluationUtility.getEffectiveBooleanValue(value).orElse(false);
	}

	void isTrue(ValuePipeQueryValueEvaluationStep step, ValuePipe parent, BindingSet bindings) {
		step.evaluate(new ConvertingValuePipe(parent, valueEval::effectiveBooleanLiteral), bindings);
	}

	boolean isTrue(Value v) {
		return valueEval.isTrue(v);
	}

	boolean hasStatement(StatementPattern sp, BindingSet bindings) throws QueryEvaluationException {
		QuadPattern nq = tupleEval.getQuadPattern(sp, bindings);
		if (nq != null) {
			ExtendedTripleSource tripleSource = (ExtendedTripleSource) tupleEval.getTripleSource(sp, bindings);
			if (tripleSource != null) {
				if (nq.isAllNamedContexts()) {
					// can't optimize for this
				    try (CloseableIteration<?, QueryEvaluationException> stmtIter = tupleEval.getStatements(nq, tripleSource)) {
				    	return stmtIter.hasNext();
				    }
				} else {
					return tripleSource.hasStatement(nq.subj, nq.pred, nq.obj, nq.ctxs);
				}
			}
		}
		return false;
	}

	@Override
	public String toString() {
		return super.toString() + "[tripleSource = " + tripleSource + "]";
	}


	public static boolean isSearchStatement(Value obj) {
		return (obj != null) && obj.isLiteral() && HALYARD.SEARCH.equals(((Literal) obj).getDatatype());
	}

    TripleSource loadFunctionGraph() {
    	// just use SimpleValueFactory for the model
    	ValueFactory vf = SimpleValueFactory.getInstance();
    	Model model = loadFunctionGraph(functionRegistry, aggregateFunctionRegistry, vf);
    	return new ModelTripleSource(model, vf);
    }

    public static Model loadFunctionGraph(FunctionRegistry functionRegistry, CustomAggregateFunctionRegistry aggregateFunctionRegistry, ValueFactory vf) {
    	// read-only LinkedHashModel doesn't need synchronising
    	Model model = new LinkedHashModel();
    	IRI builtinFunctions = vf.createIRI("builtin:Functions");
    	for (org.eclipse.rdf4j.query.algebra.evaluation.function.Function func : functionRegistry.getAll()) {
    		String funcIri = func.getURI();
    		boolean isBuiltin = (funcIri.indexOf(':') == -1);
    		if (isBuiltin) {
    			funcIri = "builtin:" + funcIri;
    		}
    		IRI subj = vf.createIRI(funcIri);
    		model.add(subj, RDF.TYPE, SD.FUNCTION);
    		model.add(subj, RDFS.SUBCLASSOF, builtinFunctions);
    	}
    	for (AggregateFunctionFactory func : aggregateFunctionRegistry.getAll()) {
    		String funcIri = func.getIri();
    		IRI subj = vf.createIRI(funcIri);
    		model.add(subj, RDF.TYPE, SD.AGGREGATE);
    	}
		RDFHandler modelInserter = new AbstractRDFHandler() {
			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				model.add(st);
			}
		};
		ClassLoader cl = ClassLoader.getSystemClassLoader();
    	cl.resources("schema/functions").forEach(url -> {
    		try {
	    		try (InputStream infIn = url.openStream()) {
	    			for(String fileName : IOUtils.readLines(infIn, StandardCharsets.UTF_8)) {
	    				RDFParser parser = Rio.createParser(Rio.getParserFormatForFileName(fileName).orElseThrow(Rio.unsupportedFormat(fileName)), vf);
	    				parser.setRDFHandler(modelInserter);
	    				try (InputStream rdfIn = cl.getResourceAsStream(fileName)) {
	    					parser.parse(rdfIn);
	    				}
	    			}
	    		}
    		} catch (IOException ioe) {
    			throw new UncheckedIOException(ioe);
    		}
    	});
    	return model;
    }


	private final class ResultSizeCountingBindingSetPipe extends BindingSetPipe {
		private final AtomicLong counter = new AtomicLong();
		private final TupleExpr queryNode;
		private volatile long lastCount;

		private ResultSizeCountingBindingSetPipe(BindingSetPipe parent, TupleExpr expr) {
			super(parent);
			this.queryNode = expr;
			// set resultsSizeActual to at least be 0 so we can track iterations that don't produce anything
			Algebra.initResultSizeActual(queryNode);
		}

		@Override
		protected boolean next(BindingSet bs) {
			long count = counter.incrementAndGet();
			if ((count - lastCount) > config.trackResultSizeUpdateInterval) {
				updateResultSize();
			}
			return super.next(bs);
		}

		@Override
		public boolean handleException(Throwable e) {
			updateResultSize();
			return parent.handleException(e);
		}

		@Override
		protected void doClose() {
			updateResultSize();
			parent.close();
		}

		private synchronized void updateResultSize() {
			long count = counter.get();
			Algebra.incrementResultSizeActual(queryNode, count - lastCount);
			lastCount = count;
		}
	}

	private final class TimedBindingSetPipe extends BindingSetPipe {
		private final AtomicLong elapsed = new AtomicLong();
		private final TupleExpr queryNode;
		private volatile long lastNanos;

		private TimedBindingSetPipe(BindingSetPipe parent, TupleExpr expr) {
			super(parent);
			this.queryNode = expr;
			Algebra.initTotalTimeNanosActual(queryNode);
		}

		@Override
		protected boolean next(BindingSet bs) {
			long start = System.nanoTime();
			boolean pushMore = super.next(bs);
			long end = System.nanoTime();
			long nanos = elapsed.addAndGet(end - start);
			if ((nanos - lastNanos) > config.trackResultTimeUpdateInterval) {
				updateResultTime();
			}
			return pushMore;
		}

		@Override
		public boolean handleException(Throwable e) {
			updateResultTime();
			return parent.handleException(e);
		}

		@Override
		protected void doClose() {
			updateResultTime();
			parent.close();
		}

		private synchronized void updateResultTime() {
			long nanos = elapsed.get();
			Algebra.incrementTotalTimeNanosActual(queryNode, nanos - lastNanos);
			lastNanos = nanos;
		}
	}

	/**
	 * This class wraps an iterator and increments the "resultSizeActual" of the query model node that the iterator
	 * represents. This means we can track the number of tuples that have been retrieved from this node.
	 */
	private final class ResultSizeCountingIterator extends IterationWrapper<BindingSet, QueryEvaluationException> {

		private final CloseableIteration<BindingSet, QueryEvaluationException> iterator;
		private final QueryModelNode queryModelNode;
		private long counter;

		private ResultSizeCountingIterator(CloseableIteration<BindingSet, QueryEvaluationException> iterator,
				QueryModelNode queryModelNode) {
			super(iterator);
			this.iterator = iterator;
			this.queryModelNode = queryModelNode;
			// set resultsSizeActual to at least be 0 so we can track iterations that don't produce anything
			Algebra.initResultSizeActual(queryModelNode);
		}

		@Override
		public BindingSet next() throws QueryEvaluationException {
			counter++;
			if (counter > config.trackResultSizeUpdateInterval) {
				updateResultSize();
			}
			return iterator.next();
		}

		@Override
		protected void handleClose() throws QueryEvaluationException {
			updateResultSize();
			super.handleClose();
		}

		private void updateResultSize() {
			Algebra.incrementResultSizeActual(queryModelNode, counter);
			counter = 0L;
		}
	}

	/**
	 * This class wraps an iterator and tracks the time used to execute next() and hasNext()
	 */
	private final class TimedIterator extends IterationWrapper<BindingSet, QueryEvaluationException> {

		private final CloseableIteration<BindingSet, QueryEvaluationException> iterator;
		private final QueryModelNode queryModelNode;
		private long elapsed;

		private TimedIterator(CloseableIteration<BindingSet, QueryEvaluationException> iterator,
				QueryModelNode queryModelNode) {
			super(iterator);
			this.iterator = iterator;
			this.queryModelNode = queryModelNode;
			Algebra.initTotalTimeNanosActual(queryModelNode);
		}

		@Override
		public BindingSet next() throws QueryEvaluationException {
			long start = System.nanoTime();
			BindingSet next = iterator.next();
			long end = System.nanoTime();
			elapsed += end - start;
			if (elapsed > config.trackResultTimeUpdateInterval) {
				updateResultTime();
			}
			return next;
		}

		@Override
		public boolean hasNext() throws QueryEvaluationException {
			long start = System.nanoTime();
			boolean hasNext = super.hasNext();
			long end = System.nanoTime();
			elapsed += end - start;
			if (elapsed > config.trackResultTimeUpdateInterval) {
				updateResultTime();
			}
			return hasNext;
		}

		@Override
		protected void handleClose() throws QueryEvaluationException {
			updateResultTime();
			super.handleClose();
		}

		private void updateResultTime() {
			Algebra.incrementTotalTimeNanosActual(queryModelNode, elapsed);
			elapsed = 0L;
		}
	}
}
