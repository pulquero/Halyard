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

import com.google.common.collect.Sets;
import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.Algorithms;
import com.msd.gin.halyard.algebra.ConstrainedStatementPattern;
import com.msd.gin.halyard.algebra.ExtendedTupleFunctionCall;
import com.msd.gin.halyard.algebra.StarJoin;
import com.msd.gin.halyard.algebra.evaluation.ConstrainedTripleSourceFactory;
import com.msd.gin.halyard.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.algebra.evaluation.ModelTripleSource;
import com.msd.gin.halyard.common.CachingValueFactory;
import com.msd.gin.halyard.common.LiteralConstraint;
import com.msd.gin.halyard.common.ValueConstraint;
import com.msd.gin.halyard.common.ValueFactories;
import com.msd.gin.halyard.common.ValueType;
import com.msd.gin.halyard.federation.BindingSetConsumerFederatedService;
import com.msd.gin.halyard.federation.BindingSetPipeFederatedService;
import com.msd.gin.halyard.optimizers.JoinAlgorithmOptimizer;
import com.msd.gin.halyard.query.AbortConsumerException;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.BindingSetPipeQueryEvaluationStep;
import com.msd.gin.halyard.query.ValuePipe;
import com.msd.gin.halyard.query.ValuePipeQueryValueEvaluationStep;
import com.msd.gin.halyard.strategy.aggregators.AvgAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.AvgCollector;
import com.msd.gin.halyard.strategy.aggregators.CSVCollector;
import com.msd.gin.halyard.strategy.aggregators.ConcatAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.CountAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.LongCollector;
import com.msd.gin.halyard.strategy.aggregators.MaxAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.MinAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.NumberCollector;
import com.msd.gin.halyard.strategy.aggregators.SampleAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.SampleCollector;
import com.msd.gin.halyard.strategy.aggregators.SumAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.ThreadSafeAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.ValueCollector;
import com.msd.gin.halyard.strategy.aggregators.WildcardCountAggregateFunction;
import com.msd.gin.halyard.strategy.collections.BigHashSet;
import com.msd.gin.halyard.strategy.collections.Sorter;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.common.iteration.UnionIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.algebra.AggregateFunctionCall;
import org.eclipse.rdf4j.query.algebra.AggregateOperator;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.Avg;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Count;
import org.eclipse.rdf4j.query.algebra.Datatype;
import org.eclipse.rdf4j.query.algebra.DescribeOperator;
import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Distinct;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.GroupConcat;
import org.eclipse.rdf4j.query.algebra.GroupElem;
import org.eclipse.rdf4j.query.algebra.Intersection;
import org.eclipse.rdf4j.query.algebra.IsNumeric;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Lang;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.Max;
import org.eclipse.rdf4j.query.algebra.Min;
import org.eclipse.rdf4j.query.algebra.MultiProjection;
import org.eclipse.rdf4j.query.algebra.Order;
import org.eclipse.rdf4j.query.algebra.OrderElem;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Reduced;
import org.eclipse.rdf4j.query.algebra.Sample;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.Slice;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Sum;
import org.eclipse.rdf4j.query.algebra.TripleRef;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryValueEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.DescribeIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.PathIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.ProjectionIterator;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtility;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.VarNameCollector;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateCollector;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates {@link TupleExpr}s and it's sub-interfaces and implementations.
 * @author Adam Sotona (MSD)
 */
final class HalyardTupleExprEvaluation {
	private static final Logger LOGGER = LoggerFactory.getLogger(HalyardTupleExprEvaluation.class);
	private static final String ANON_SUBJECT_VAR = "__subj";
	private static final String ANON_PREDICATE_VAR = "__pred";
	private static final String ANON_OBJECT_VAR = "__obj";
	private static final int MAX_INITIAL_HASH_JOIN_TABLE_SIZE = 5000;
	private static final Resource[] ALL_CONTEXTS = new Resource[0];
	private static final Set<IRI> VIRTUAL_CONTEXTS = Sets.newHashSet(HALYARD.FUNCTION_GRAPH_CONTEXT);

    private final HalyardEvaluationStrategy parentStrategy;
	private final TripleSource tripleSource;
	private final Dataset dataset;
    private final HalyardEvaluationExecutor executor;
    private final int hashJoinLimit;
    private final int collectionMemoryThreshold;
    private final int valueCacheSize;
    private volatile TripleSource functionGraph;

    /**
	 * Constructor used by {@link HalyardEvaluationStrategy} to create this helper class
	 * 
	 * @param parentStrategy
	 * @param tupleFunctionRegistry
	 * @param tripleSource
	 * @param dataset
	 * @param executor
	 */
	HalyardTupleExprEvaluation(HalyardEvaluationStrategy parentStrategy,
			TripleSource tripleSource, Dataset dataset, HalyardEvaluationExecutor executor) {
        this.parentStrategy = parentStrategy;
		this.tripleSource = tripleSource;
		this.dataset = dataset;
		this.executor = executor;
		StrategyConfig config = parentStrategy.getConfig();
		JoinAlgorithmOptimizer algoOpt = parentStrategy.getJoinAlgorithmOptimizer();
    	if (algoOpt != null) {
    		hashJoinLimit = algoOpt.getHashJoinLimit();
    	} else {
    		hashJoinLimit = config.hashJoinLimit;
    	}
    	collectionMemoryThreshold = config.collectionMemoryThreshold;
    	valueCacheSize = config.valueCacheSize;
    }

    /**
     * Precompiles the given expression.
     * @param expr supplied by HalyardEvaluationStrategy
     * @param evalContext
     * @return a QueryEvaluationStep
     */
	BindingSetPipeQueryEvaluationStep precompile(TupleExpr expr, QueryEvaluationContext evalContext) {
    	BindingSetPipeEvaluationStep step = precompileTupleExpr(expr, evalContext);
    	return new BindingSetPipeQueryEvaluationStep() {
			@Override
			public void evaluate(BindingSetPipe parent, BindingSet bindings) {
				executor.push(parent, step, bindings);
			}

			@Override
			public void evaluate(Consumer<BindingSet> handler, BindingSet bindings) {
				executor.pushAndPullSync(handler, step, bindings);
			}

			@Override
			public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) {
				return executor.pushAndPullAsync(step, expr, bindings);
			}
    	};
    }

    /**
     * Switch logic appropriate for each type of {@link TupleExpr} query model node, sending each type to it's appropriate precompile method. For example,
     * {@code UnaryTupleOperator} is sent to {@link precompileUnaryTupleOperator()}.
     */
    private BindingSetPipeEvaluationStep precompileTupleExpr(TupleExpr expr, QueryEvaluationContext evalContext) {
        if (expr instanceof StatementPattern) {
            return precompileStatementPattern((StatementPattern) expr);
        } else if (expr instanceof UnaryTupleOperator) {
        	return precompileUnaryTupleOperator((UnaryTupleOperator) expr, evalContext);
        } else if (expr instanceof BinaryTupleOperator) {
        	return precompileBinaryTupleOperator((BinaryTupleOperator) expr, evalContext);
        } else if (expr instanceof StarJoin) {
        	return precompileStarJoin((StarJoin) expr, evalContext);
        } else if (expr instanceof SingletonSet) {
        	return precompileSingletonSet((SingletonSet) expr);
        } else if (expr instanceof EmptySet) {
        	return precompileEmptySet((EmptySet) expr);
        } else if (expr instanceof ZeroLengthPath) {
        	return precompileZeroLengthPath((ZeroLengthPath) expr);
        } else if (expr instanceof ArbitraryLengthPath) {
        	return precompileArbitraryLengthPath((ArbitraryLengthPath) expr);
        } else if (expr instanceof BindingSetAssignment) {
        	return precompileBindingSetAssignment((BindingSetAssignment) expr);
        } else if (expr instanceof TripleRef) {
        	return precompileTripleRef((TripleRef) expr);
		} else if (expr instanceof TupleFunctionCall) {
			// all TupleFunctionCalls are expected to be ExtendedTupleFunctionCalls
			return precompileTupleFunctionCall((ExtendedTupleFunctionCall) expr, evalContext);
        } else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        } else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }

    /**
     * Switch logic for precompilation of any instance of a {@link UnaryTupleOperator} query model node
     */
    private BindingSetPipeEvaluationStep precompileUnaryTupleOperator(UnaryTupleOperator expr, QueryEvaluationContext evalContext) {
        if (expr instanceof Projection) {
        	return precompileProjection((Projection) expr, evalContext);
        } else if (expr instanceof MultiProjection) {
        	return precompileMultiProjection((MultiProjection) expr, evalContext);
        } else if (expr instanceof Filter) {
        	return precompileFilter((Filter) expr, evalContext);
        } else if (expr instanceof Service) {
        	return precompileService((Service) expr);
        } else if (expr instanceof Slice) {
        	return precompileSlice((Slice) expr, evalContext);
        } else if (expr instanceof Extension) {
        	return precompileExtension((Extension) expr, evalContext);
        } else if (expr instanceof Distinct) {
        	return precompileDistinct((Distinct) expr, evalContext);
        } else if (expr instanceof Reduced) {
        	return precompileReduced((Reduced) expr, evalContext);
        } else if (expr instanceof Group) {
        	return precompileGroup((Group) expr, evalContext);
        } else if (expr instanceof Order) {
        	return precompileOrder((Order) expr, evalContext);
        } else if (expr instanceof QueryRoot) {
        	QueryRoot root = (QueryRoot) expr;
        	parentStrategy.sharedValueOfNow.set(null);
        	BindingSetPipeEvaluationStep step = precompileTupleExpr(root.getArg(), evalContext);
        	return (parent, bindings) -> {
        		step.evaluate(parentStrategy.track(parent, root), bindings);
        	};
        } else if (expr instanceof DescribeOperator) {
        	return precompileDescribeOperator((DescribeOperator) expr, evalContext);
        } else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        } else {
            throw new QueryEvaluationException("Unknown unary tuple operator type: " + expr.getClass());
        }
    }

    private BindingSetPipeEvaluationStep precompileStatementPattern(StatementPattern sp) {
    	return (parent, bindings) -> evaluateStatementPattern(parent, sp, bindings);
    }

    TripleSource getTripleSource(StatementPattern sp, BindingSet bindings) {
    	if ((sp instanceof ConstrainedStatementPattern) && (tripleSource instanceof ConstrainedTripleSourceFactory)) {
    		ConstrainedStatementPattern csp = (ConstrainedStatementPattern) sp;
    		ValueConstraint subjConstraint = null;
    		if (csp.getSubjectType() != null) {
    			subjConstraint = new ValueConstraint(csp.getSubjectType());
    		}
    		ValueConstraint objConstraint = null;
    		if (csp.getObjectType() != null) {
				if (csp.getObjectType() == ValueType.LITERAL) {
					UnaryValueOperator constraintFunc = csp.getLiteralConstraintFunction();
					if (constraintFunc != null) {
						ValueExpr constraintValue = csp.getLiteralConstraintValue();
						Value v;
						if (constraintValue instanceof ValueConstant) {
							v = ((ValueConstant) constraintValue).getValue();
						} else if (constraintValue instanceof Var) {
							v = Algebra.getVarValue((Var) constraintValue, bindings);
						} else {
							v = null;
						}
						if (v != null) {
							if (constraintFunc instanceof Datatype) {
								if (!v.isIRI()) {
									return null; // nothing to push
								}
								IRI dt = (IRI) v;
				    			objConstraint = new LiteralConstraint(dt);
							} else if (constraintFunc instanceof Lang) {
								if (!v.isLiteral()) {
									return null; // nothing to push
								}
								Literal langValue = (Literal) v;
								String lang = langValue.getLabel();
								if (!lang.isEmpty()) {
					    			objConstraint = new LiteralConstraint(lang);
								} else {
					    			objConstraint = new ValueConstraint(ValueType.LITERAL);
								}
							} else if ((constraintFunc instanceof IsNumeric) && BooleanLiteral.TRUE.equals(v)) {
				    			objConstraint = new LiteralConstraint(HALYARD.ANY_NUMERIC_TYPE);
							}
						}
	    			} else {
		    			objConstraint = new ValueConstraint(ValueType.LITERAL);
	    			}
	    		} else {
	    			objConstraint = new ValueConstraint(csp.getObjectType());
	    		}
    		}
			return ((ConstrainedTripleSourceFactory)tripleSource).getTripleSource(subjConstraint, objConstraint);
    	}
    	return tripleSource;
    }

    /**
     * Evaluate the statement pattern using the supplied bindings
     * @param parent to push or enqueue evaluation results
     * @param sp the {@code StatementPattern} to evaluate
     * @param bindings the set of names to which values are bound. For example, select ?s, ?p, ?o has the names s, p and o and the values bound to them are the
     * results of the evaluation of this statement pattern
     */
    private void evaluateStatementPattern(final BindingSetPipe parent, final StatementPattern sp, final BindingSet bindings) {
        QuadPattern nq = getQuadPattern(sp, bindings);
        if (nq != null) {
    		TripleSource ts = getTripleSource(sp, bindings);
    		if (ts != null) {
    	        QueryEvaluationStep evalStep = evaluateStatementPattern(sp, nq, ts);
        		try {
    				executor.pullAndPushAsync(parent, evalStep, sp, bindings, parentStrategy);
                } catch (QueryEvaluationException e) {
                    parent.handleException(e);
                }
    		} else {
    			parent.close(); // nothing to push
    		}
		} else {
			parent.close(); // nothing to push
		}
    }

    static final class QuadPattern {
    	final Resource subj;
    	final IRI pred;
    	final Value obj;
    	final Resource[] ctxs;
    	final StatementPattern.Scope scope;

    	public QuadPattern(Resource subj, IRI pred, Value obj, Resource[] ctxs, StatementPattern.Scope scope) {
			this.subj = subj;
			this.pred = pred;
			this.obj = obj;
			this.ctxs = ctxs;
			this.scope = scope;
		}

    	boolean isAllNamedContexts() {
    		return (scope == StatementPattern.Scope.NAMED_CONTEXTS) && (ctxs.length == 0);
    	}
    }

    QuadPattern getQuadPattern(StatementPattern sp, BindingSet bindings) {
        final Var subjVar = sp.getSubjectVar(); //subject
        final Var predVar = sp.getPredicateVar(); //predicate
        final Var objVar = sp.getObjectVar(); //object
        final Var conVar = sp.getContextVar(); //graph or target context
        final StatementPattern.Scope scope = sp.getScope();

        final Resource subjValue;
        final IRI predValue;
        final Value objValue;
        final Resource contextValue;
    	try {
	        subjValue = (Resource) Algebra.getVarValue(subjVar, bindings);
	        predValue = (IRI) Algebra.getVarValue(predVar, bindings);
	        objValue = Algebra.getVarValue(objVar, bindings);
	        contextValue = (Resource) Algebra.getVarValue(conVar, bindings);
        } catch (ClassCastException e) {
            // Invalid value type for subject, predicate and/or context
            return null;
        }

        if (isUnbound(subjVar, bindings) || isUnbound(predVar, bindings) || isUnbound(objVar, bindings) || isUnbound(conVar, bindings)) {
            // the variable must remain unbound for this solution see https://www.w3.org/TR/sparql11-query/#assignment
            return null;
        }

        final Set<IRI> graphs;
        final boolean emptyGraph;
        if (dataset != null) {
            if (scope == StatementPattern.Scope.DEFAULT_CONTEXTS) { //evaluate against the default graph(s)
                graphs = dataset.getDefaultGraphs();
                emptyGraph = graphs.isEmpty() && !dataset.getNamedGraphs().isEmpty();
            } else { //evaluate against the named graphs
                graphs = dataset.getNamedGraphs();
                emptyGraph = graphs.isEmpty() && !dataset.getDefaultGraphs().isEmpty();
            }
        } else {
        	graphs = null;
        	emptyGraph = false;
        }

        final Resource[] contexts;
        if (emptyGraph) {
            // Search zero contexts
            contexts = null; //no results from this statement pattern
        } else if (graphs == null || graphs.isEmpty()) {
            // store default behaviour
            if (contextValue != null) {
				if (RDF4J.NIL.equals(contextValue) || SESAME.NIL.equals(contextValue)) {
					contexts = new Resource[] { null };
				} else {
					contexts = new Resource[] { contextValue };
				}
            }
			/*
			 * TODO activate this to have an exclusive (rather than inclusive) interpretation of the default
			 * graph in SPARQL querying. else if (scope == StatementPattern.Scope.DEFAULT_CONTEXTS ) {
			 * contexts = new Resource[] { null }; }
			 */
			else {
				contexts = ALL_CONTEXTS;
			}
        } else if (contextValue != null) {
            if (graphs.contains(contextValue)) {
                contexts = new Resource[] { contextValue };
            } else {
                // Statement pattern specifies a context that is not part of
                // the dataset
            	contexts = null; //no results possible because the context is not available
            }
        } else {
            contexts = new Resource[graphs.size()];
            int i = 0;
            for (IRI graph : graphs) {
                IRI context;
				if (RDF4J.NIL.equals(graph) || SESAME.NIL.equals(graph)) {
                    context = null;
                } else {
                	context = graph;
                }
                contexts[i++] = context;
            }
        }

        if (contexts == null) {
        	return null;
        }

        return new QuadPattern(subjValue, predValue, objValue, contexts, scope);
    }

    private static boolean isUnbound(Var var, BindingSet bindings) {
        if (var == null) {
            return false;
        } else {
            return bindings.hasBinding(var.getName()) && bindings.getValue(var.getName()) == null;
        }
    }

    CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(QuadPattern nq, TripleSource tripleSource) {
    	Resource[] mainCtxs;
    	List<Resource> virtualCtxs;
    	if (nq.ctxs.length == 0) {
    		mainCtxs = nq.ctxs;
    		virtualCtxs = Collections.emptyList();
    	} else {
	    	List<Resource> mainCtxList = new ArrayList<>(nq.ctxs.length);
	    	virtualCtxs = new ArrayList<>(1);
	    	for (Resource ctx : nq.ctxs) {
	    		if (VIRTUAL_CONTEXTS.contains(ctx)) {
	    			virtualCtxs.add(ctx);
	    		} else {
	    			mainCtxList.add(ctx);
	    		}
	    	}
	    	if (virtualCtxs.isEmpty()) {
	    		mainCtxs = nq.ctxs;
	    	} else {
	    		mainCtxs = !mainCtxList.isEmpty() ? mainCtxList.toArray(new Resource[mainCtxList.size()]) : null;
	    	}
    	}

    	CloseableIteration<? extends Statement, QueryEvaluationException> mainIter;
    	if (mainCtxs != null) {
	    	mainIter = tripleSource.getStatements(nq.subj, nq.pred, nq.obj, mainCtxs);
	        if (nq.isAllNamedContexts()) {
	            // Named contexts are matched by retrieving all statements from
	            // the store and filtering out the statements that do not have a context.
	            mainIter = new FilterIteration<Statement, QueryEvaluationException>(mainIter) {
	
	                @Override
	                protected boolean accept(Statement st) {
	                    return st.getContext() != null;
	                }
	
	            };
	        }
    	} else {
    		mainIter = null;
    	}

        if (!virtualCtxs.isEmpty()) {
    		List<CloseableIteration<? extends Statement, QueryEvaluationException>> iters = new ArrayList<>(1 + virtualCtxs.size());
    		if (mainIter != null) {
    			iters.add(mainIter);
    		}
    		for (Resource ctx : virtualCtxs) {
    			if (HALYARD.FUNCTION_GRAPH_CONTEXT.equals(ctx)) {
    				iters.add(getFunctionGraph().getStatements(nq.subj, nq.pred, nq.obj));
    			}
    		}
    		return iters.size() > 1 ? new UnionIteration<>(iters) : iters.get(0);
    	} else {
    		return mainIter;
    	}
    }

    private TripleSource getFunctionGraph() {
		TripleSource localRef = functionGraph;
		if (localRef == null) {
			synchronized (this) {
				localRef = functionGraph;
				if (localRef == null) {
					localRef = loadFunctionGraph();
					functionGraph = localRef;
				}
			}
		}
		return localRef;
    }

    private TripleSource loadFunctionGraph() {
    	// just use SimpleValueFactory for the model
    	ValueFactory vf = SimpleValueFactory.getInstance();
    	// read-only LinkedHashModel doesn't need synchronising
    	Model model = new LinkedHashModel();
    	IRI builtinFunctions = vf.createIRI("builtin:Functions");
    	for (org.eclipse.rdf4j.query.algebra.evaluation.function.Function func : parentStrategy.functionRegistry.getAll()) {
    		String funcIri = func.getURI();
    		boolean isBuiltin = (funcIri.indexOf(':') == -1);
    		if (isBuiltin) {
    			funcIri = "builtin:" + funcIri;
    		}
    		IRI subj = vf.createIRI(funcIri);
    		model.add(subj, RDF.TYPE, SD.FUNCTION);
    		model.add(subj, RDFS.SUBCLASSOF, builtinFunctions);
    	}
    	for (AggregateFunctionFactory func : parentStrategy.aggregateFunctionRegistry.getAll()) {
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
    	getClass().getClassLoader().resources("schema/functions").forEach(url -> {
    		try {
	    		try (InputStream infIn = url.openStream()) {
	    			for(String fileName : IOUtils.readLines(infIn, StandardCharsets.UTF_8)) {
	    				RDFParser parser = Rio.createParser(Rio.getParserFormatForFileName(fileName).orElseThrow(Rio.unsupportedFormat(fileName)), vf);
	    				parser.setRDFHandler(modelInserter);
	    				try (InputStream rdfIn = getClass().getClassLoader().getResourceAsStream(fileName)) {
	    					parser.parse(rdfIn);
	    				}
	    			}
	    		}
    		} catch (IOException ioe) {
    			throw new UncheckedIOException(ioe);
    		}
    	});
    	return new ModelTripleSource(model, vf);
    }

    private QueryEvaluationStep evaluateStatementPattern(StatementPattern sp, QuadPattern nq, TripleSource tripleSource) {
        final Var ctxVar = sp.getContextVar();

        return bs -> {
	        //get an iterator over all triple statements that match the s, p, o specification in the contexts
	        CloseableIteration<? extends Statement, QueryEvaluationException> stIter = getStatements(nq, tripleSource);
	
	        // The same variable might have been used multiple times in this
	        // StatementPattern (e.g. ?x :p ?x or ?x ?x "foobar"), verify value equality in those cases.
	        int distinctVarCount = sp.getBindingNames().size();
	        boolean allVarsDistinct = (ctxVar != null && distinctVarCount == 4) || (ctxVar == null && distinctVarCount == 3);
	        if (!allVarsDistinct) {
		        stIter = new FilterIteration<Statement, QueryEvaluationException>(stIter) {
		            @Override
		            protected boolean accept(Statement stmt) {
		            	return filterStatement(sp, stmt, nq);
		            }
		        };
	        }
	
	        // Return an iterator that converts the RDF statements (triples) to var bindings
	        return new ConvertingIteration<Statement, BindingSet, QueryEvaluationException>(stIter) {
	            @Override
	            protected BindingSet convert(Statement stmt) {
	            	return convertStatement(sp, stmt, bs);
	            }
			};
        };
    }

    private boolean filterStatement(StatementPattern sp, Statement stmt, QuadPattern nq) {
        final Var subjVar = sp.getSubjectVar();
        final Var predVar = sp.getPredicateVar();
        final Var objVar = sp.getObjectVar();
        final Var ctxVar = sp.getContextVar();
        Resource subj = stmt.getSubject();
        IRI pred = stmt.getPredicate();
        Value obj = stmt.getObject();
        Resource context = stmt.getContext();

        if (nq.subj == null) {
            if (subjVar.equals(predVar) && !subj.equals(pred)) {
                return false;
            }
            if (subjVar.equals(objVar) && !subj.equals(obj)) {
                return false;
            }
            if (subjVar.equals(ctxVar) && !subj.equals(context)) {
                return false;
            }
        }

        if (nq.pred == null) {
            if (predVar.equals(objVar) && !pred.equals(obj)) {
                return false;
            }
            if (predVar.equals(ctxVar) && !pred.equals(context)) {
                return false;
            }
        }

        if (nq.obj == null) {
            if (objVar.equals(ctxVar) && !obj.equals(context)) {
                return false;
            }
        }

        return true;
    }

    private MutableBindingSet convertStatement(StatementPattern sp, Statement stmt, BindingSet bs) {
        final Var subjVar = sp.getSubjectVar();
        final Var predVar = sp.getPredicateVar();
        final Var objVar = sp.getObjectVar();
        final Var ctxVar = sp.getContextVar();

        QueryBindingSet result = new QueryBindingSet(bs);
        if (!subjVar.isConstant()) {
            final String subjName = subjVar.getName();
            if (!result.hasBinding(subjName)) {
                result.addBinding(subjName, stmt.getSubject());
            }
        }
        if (!predVar.isConstant()) {
            final String predName = predVar.getName();
            if (!result.hasBinding(predName)) {
                result.addBinding(predName, stmt.getPredicate());
            }
        }
        if (!objVar.isConstant()) {
            final String objName = objVar.getName();
            if (!result.hasBinding(objName)) {
                result.addBinding(objVar.getName(), stmt.getObject());
            } else {
                Value val = result.getValue(objName);
                if (HalyardEvaluationStrategy.isSearchStatement(val)) {
                    // override Halyard search type object literals with real object value from the statement
                    result.setBinding(objName, stmt.getObject());
                }
            }
        }
        if (ctxVar != null && !ctxVar.isConstant() && stmt.getContext() != null) {
            final String ctxName = (ctxVar != null) ? ctxVar.getName() : null;
            if (!result.hasBinding(ctxName)) {
                result.addBinding(ctxName, stmt.getContext());
            }
        }
    	return result;
    }

    /**
	 * Precompiles a TripleRef node returning bindingsets from the matched Triple nodes in the dataset (or explore
	 * standard reification)
	 */
	private BindingSetPipeEvaluationStep precompileTripleRef(TripleRef tripleRef) {
		// Naive implementation that walks over all statements matching (x rdf:type rdf:Statement)
		// and filter those that do not match the bindings for subject, predicate and object vars (if bound)
		final Var subjVar = tripleRef.getSubjectVar();
		final Var predVar = tripleRef.getPredicateVar();
		final Var objVar = tripleRef.getObjectVar();
		final Var extVar = tripleRef.getExprVar();
		// whether the TripleSouce support access to RDF star
		final boolean sourceSupportsRdfStar = tripleSource instanceof RDFStarTripleSource;
		if (sourceSupportsRdfStar) {
			return (parent, bindings) -> {
				final Value subjValue = Algebra.getVarValue(subjVar, bindings);
				final Value predValue = Algebra.getVarValue(predVar, bindings);
				final Value objValue = Algebra.getVarValue(objVar, bindings);
				final Value extValue = Algebra.getVarValue(extVar, bindings);

				// case1: when we have a binding for extVar we use it in the reified nodes lookup
				// case2: in which we have unbound extVar
				// in both cases:
				// 1. iterate over all statements matching ((* | extValue), rdf:type, rdf:Statement)
				// 2. construct a look ahead iteration and filter these solutions that do not match the
				// bindings for the subject, predicate and object vars (if these are bound)
				// return set of solution where the values of the statements (extVar, rdf:subject/predicate/object, value)
				// are bound to the variables of the respective TripleRef variables for subject, predicate, object
				// NOTE: if the tripleSource is extended to allow for lookup over asserted Triple values in the underlying sail
				// the evaluation of the TripleRef should be suitably forwarded down the sail and filter/construct
				// the correct solution out of the results of that call
				if (extValue != null && !(extValue instanceof Triple)) {
					parent.close(); // nothing to push
					return;
				}

				if (extValue != null) {
					Triple extTriple = (Triple) extValue;
					parent = parentStrategy.track(parent, tripleRef);
					QueryBindingSet result = new QueryBindingSet(bindings);
					if (subjValue == null) {
						result.addBinding(subjVar.getName(), extTriple.getSubject());
					} else if (!subjValue.equals(extTriple.getSubject())) {
						parent.close(); // nothing to push
						return;
					}
					if (predValue == null) {
						result.addBinding(predVar.getName(), extTriple.getPredicate());
					} else if (!predValue.equals(extTriple.getPredicate())) {
						parent.close(); // nothing to push
						return;
					}
					if (objValue == null) {
						result.addBinding(objVar.getName(), extTriple.getObject());
					} else if (!objValue.equals(extTriple.getObject())) {
						parent.close(); // nothing to push
						return;
					}
					parent.pushLast(result);
				} else {
					final QueryEvaluationStep evalStep = bs -> {
						CloseableIteration<? extends Triple, QueryEvaluationException> sourceIter = ((RDFStarTripleSource) tripleSource)
								.getRdfStarTriples((Resource) subjValue, (IRI) predValue, objValue);
						return new ConvertingIteration<Triple, BindingSet, QueryEvaluationException>(sourceIter) {
							@Override
							protected BindingSet convert(Triple triple) {
								QueryBindingSet result = new QueryBindingSet(bs);
								if (subjValue == null) {
									result.addBinding(subjVar.getName(), triple.getSubject());
								}
								if (predValue == null) {
									result.addBinding(predVar.getName(), triple.getPredicate());
								}
								if (objValue == null) {
									result.addBinding(objVar.getName(), triple.getObject());
								}
								result.addBinding(extVar.getName(), triple);
								return result;
							}
						};
					};
					executor.pullAndPushAsync(parent, evalStep, tripleRef, bindings, parentStrategy);
				}
			};
		} else {
			return (parent, bindings) -> {
				final Value subjValue = Algebra.getVarValue(subjVar, bindings);
				final Value predValue = Algebra.getVarValue(predVar, bindings);
				final Value objValue = Algebra.getVarValue(objVar, bindings);
				final Value extValue = Algebra.getVarValue(extVar, bindings);

				// case1: when we have a binding for extVar we use it in the reified nodes lookup
				// case2: in which we have unbound extVar
				// in both cases:
				// 1. iterate over all statements matching ((* | extValue), rdf:type, rdf:Statement)
				// 2. construct a look ahead iteration and filter these solutions that do not match the
				// bindings for the subject, predicate and object vars (if these are bound)
				// return set of solution where the values of the statements (extVar, rdf:subject/predicate/object, value)
				// are bound to the variables of the respective TripleRef variables for subject, predicate, object
				// NOTE: if the tripleSource is extended to allow for lookup over asserted Triple values in the underlying sail
				// the evaluation of the TripleRef should be suitably forwarded down the sail and filter/construct
				// the correct solution out of the results of that call
				if (extValue != null && !(extValue instanceof Triple)) {
					parent.close(); // nothing to push
					return;
				}

				final QueryEvaluationStep evalStep;
				// standard reification iteration
				evalStep = bs -> {
					// 1. walk over resources used as subjects of (x rdf:type rdf:Statement)
					final CloseableIteration<? extends Resource, QueryEvaluationException> iter = new ConvertingIteration<Statement, Resource, QueryEvaluationException>(
							tripleSource.getStatements((Resource) extValue, RDF.TYPE, RDF.STATEMENT)) {
		
						@Override
						protected Resource convert(Statement sourceObject) {
							return sourceObject.getSubject();
						}
					};
					// for each reification node, fetch and check the subject, predicate and object values against
					// the expected values from TripleRef pattern and supplied bindings collection
					return new LookAheadIteration<BindingSet, QueryEvaluationException>() {
						@Override
						protected void handleClose()
								throws QueryEvaluationException {
							super.handleClose();
							iter.close();
						}
		
						@Override
						protected BindingSet getNextElement()
								throws QueryEvaluationException {
							while (iter.hasNext()) {
								Resource theNode = iter.next();
								QueryBindingSet result = new QueryBindingSet(bs);
								// does it match the subjectValue/subjVar
								if (!matchValue(theNode, subjValue, subjVar, result, RDF.SUBJECT)) {
									continue;
								}
								// the predicate, if not, remove the binding that hass been added
								// when the subjValue has been checked and its value added to the solution
								if (!matchValue(theNode, predValue, predVar, result, RDF.PREDICATE)) {
									continue;
								}
								// check the object, if it do not match
								// remove the bindings added for subj and pred
								if (!matchValue(theNode, objValue, objVar, result, RDF.OBJECT)) {
									continue;
								}
								// add the extVar binding if we do not have a value bound.
								if (extValue == null) {
									result.addBinding(extVar.getName(), theNode);
								} else if (!extValue.equals(theNode)) {
									// the extVar value do not match theNode
									continue;
								}
								return result;
							}
							return null;
						}
		
						private boolean matchValue(Resource theNode, Value value, Var var, QueryBindingSet result,
								IRI predicate) {
							try (CloseableIteration<? extends Statement, QueryEvaluationException> valueiter = tripleSource
									.getStatements(theNode, predicate, null)) {
								while (valueiter.hasNext()) {
									Statement valueStatement = valueiter.next();
									if (theNode.equals(valueStatement.getSubject())) {
										if (value == null || value.equals(valueStatement.getObject())) {
											if (value == null) {
												result.addBinding(var.getName(), valueStatement.getObject());
											}
											return true;
										}
									}
								}
								return false;
							}
						}
					};
				};
				executor.pullAndPushAsync(parent, evalStep, tripleRef, bindings, parentStrategy);
			};
		}
	}

    /**
     * Precompile a {@link Projection} query model nodes
     * @param projection
     */
    private BindingSetPipeEvaluationStep precompileProjection(final Projection projection, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(projection.getArg(), evalContext);
        boolean outer = true;
        QueryModelNode ancestor = projection;
        while (ancestor.getParentNode() != null) {
                ancestor = ancestor.getParentNode();
                if (ancestor instanceof Projection || ancestor instanceof MultiProjection) {
                        outer = false;
                        break;
                }
        }
        final boolean includeAll = !outer;
    	return (parent, bindings) -> {
	        step.evaluate(new BindingSetPipe(parent) {
	            @Override
	            protected boolean next(BindingSet bs) {
	                return parent.push(ProjectionIterator.project(projection.getProjectionElemList(), bs, bindings, includeAll));
	            }
	            @Override
	            public String toString() {
	            	return "ProjectionBindingSetPipe";
	            }
	        }, bindings);
    	};
    }

    /**
     * Precompile a {@link MultiProjection} query model nodes
     * @param multiProjection
     */
    private BindingSetPipeEvaluationStep precompileMultiProjection(final MultiProjection multiProjection, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(multiProjection.getArg(), evalContext);
        return (parent, bindings) -> {
	        step.evaluate(new BindingSetPipe(parent) {
	            final List<ProjectionElemList> projections = multiProjection.getProjections();
	            final AtomicReferenceArray<BindingSet> previouslyPushed = new AtomicReferenceArray<>(projections.size());
	
	            @Override
	            protected boolean next(BindingSet bs) {
	                for (int i=0; i<projections.size(); i++) {
	                    BindingSet nb = ProjectionIterator.project(projections.get(i), bs, bindings);
	                    //ignore duplicates
	                    BindingSet prev = previouslyPushed.get(i);
                        if (!nb.equals(prev)) {
                        	previouslyPushed.compareAndSet(i, prev, nb);
	                        if (!parent.push(nb)) {
	                            return false;
	                        }
                        }
	                }
	                return true;
	            }
	            @Override
	            public String toString() {
	            	return "MultiProjectionBindingSetPipe";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompile filter {@link ExpressionTuple}s query model nodes pushing the result to the parent BindingSetPipe.
     * @param filter holds the details of any FILTER expression in a SPARQL query and any sub-chains.
     */
    private BindingSetPipeEvaluationStep precompileFilter(final Filter filter, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep argStep = precompileTupleExpr(filter.getArg(), evalContext);
        ValuePipeQueryValueEvaluationStep conditionStep = parentStrategy.precompile(filter.getCondition(), evalContext);
        return (parent, bindings) -> {
	        argStep.evaluate(new PipeJoin(parentStrategy.track(parent, filter)) {
	            @Override
	            protected boolean next(BindingSet bs) {
                    startSecondaryPipe();
	            	conditionStep.evaluate(new BindingSetValuePipe(parent) {
	            		@Override
	            		protected void next(Value v) {
	            			if (QueryEvaluationUtility.getEffectiveBooleanValue(v).orElse(false)) {
		                        pushToParent(bs);
	            			}
	            			endSecondaryPipe();
	            		}
	            		@Override
	            		public void handleValueError(String msg) {
	            			// ignore - failed to evaluate condition
	            			endSecondaryPipe();
	            		}
	            	}, bs);
	            	return !parent.isClosed();
	            }
	            @Override
	            public String toString() {
	            	return "FilterBindingSetPipe";
	            }
	        }, bindings);
	    };
    }

    /**
     * Precompile {@link DescribeOperator} query model nodes
     * @param operator
     */
    private BindingSetPipeEvaluationStep precompileDescribeOperator(DescribeOperator operator, QueryEvaluationContext evalContext) {
		BindingSetPipeQueryEvaluationStep argStep = precompile(operator.getArg(), evalContext);
		return (parent, bindings) -> {
			executor.pullAndPushAsync(parent, bs -> new DescribeIteration(argStep.evaluate(bs), parentStrategy,
				operator.getBindingNames(), bs), operator, bindings, parentStrategy);
        };
    }

	/**
     * Makes a {@link BindingSet} comparable
     * @author schremar
     *
     */
    private static class ComparableBindingSetWrapper implements Comparable<ComparableBindingSetWrapper>, Serializable {

        private static final long serialVersionUID = -7341340704807086829L;

        private static final ValueComparator VC = new ValueComparator();

        private final BindingSet bs;
        private final Value values[];
        private final boolean ascending[];
        private final long minorOrder;

        ComparableBindingSetWrapper(EvaluationStrategy strategy, BindingSet bs, QueryValueEvaluationStep[] elemSteps, boolean[] ascending, long minorOrder) throws QueryEvaluationException {
            this.bs = bs;
            this.values = new Value[elemSteps.length];
            for (int i = 0; i < values.length; i++) {
                try {
                    values[i] = elemSteps[i].evaluate(bs);
                } catch (ValueExprEvaluationException exc) {
                    values[i] = null;
                }
            }
            this.ascending = ascending;
            this.minorOrder = minorOrder;
        }

        @Override
        public int compareTo(ComparableBindingSetWrapper o) {
            if (bs.equals(o.bs)) return 0;
            for (int i=0; i<values.length; i++) {
                int cmp = ascending[i] ? VC.compare(values[i], o.values[i]) : VC.compare(o.values[i], values[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Long.compare(minorOrder, o.minorOrder);
        }

        @Override
        public int hashCode() {
            return bs.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ComparableBindingSetWrapper && bs.equals(((ComparableBindingSetWrapper)obj).bs);
        }
    }

    /**
     * Precompile {@link Order} query model nodes
     * @param order
     */
    private BindingSetPipeEvaluationStep precompileOrder(final Order order, QueryEvaluationContext evalContext) {
        final Sorter<ComparableBindingSetWrapper> sorter = new Sorter<>(getLimit(order), isReducedOrDistinct(order), collectionMemoryThreshold);
        List<OrderElem> orderElems = order.getElements();
        QueryValueEvaluationStep[] elemSteps = new QueryValueEvaluationStep[orderElems.size()];
        boolean[] ascending = new boolean[elemSteps.length];
        for (int i=0; i<elemSteps.length; i++) {
        	OrderElem oe = orderElems.get(i);
        	elemSteps[i] = parentStrategy.precompile(oe.getExpr(), evalContext);
        	ascending[i] = oe.isAscending();
        }
        BindingSetPipeEvaluationStep step = precompileTupleExpr(order.getArg(), evalContext);
        return (parent, bindings) -> {
	        step.evaluate(new BindingSetPipe(parent) {
	            final AtomicLong minorOrder = new AtomicLong();
	
	            @Override
	            public boolean handleException(Throwable e) {
	                sorter.close();
	                return parent.handleException(e);
	            }
	
	            @Override
	            protected boolean next(BindingSet bs) {
	                try {
	                    ComparableBindingSetWrapper cbsw = new ComparableBindingSetWrapper(parentStrategy, bs, elemSteps, ascending, minorOrder.getAndIncrement());
	                    sorter.add(cbsw);
	                    return true;
	                } catch (QueryEvaluationException | IOException e) {
	                    return handleException(e);
	                }
	            }

	        	
	            private void pushOrdered() {
                    for (Map.Entry<ComparableBindingSetWrapper, Long> me : sorter) {
                        for (long i = me.getValue(); i > 0; i--) {
                            if (!parent.push(me.getKey().bs)) {
                                return;
                            }
                        }
                    }
	            }

	            @Override
	            protected void doClose() {
	            	pushOrdered();
                    sorter.close();
                    parent.close();
	            }
	
	            @Override
	            public String toString() {
	            	return "OrderBindingSetPipe";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompile {@link Group} query model nodes
     * @param group
     */
    private BindingSetPipeEvaluationStep precompileGroup(Group group, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(group.getArg(), evalContext);
        List<GroupElem> elems = group.getGroupElements();
        ValuePipeQueryValueEvaluationStep[] opArgSteps = new ValuePipeQueryValueEvaluationStep[elems.size()];
        for (int i=0; i<opArgSteps.length; i++) {
        	AggregateOperator op = elems.get(i).getOperator();
        	if (op instanceof UnaryValueOperator) {
        		ValueExpr arg = ((UnaryValueOperator)op).getArg();
        		if (arg != null) {
        			opArgSteps[i] = parentStrategy.precompile(arg, evalContext);
        		}
        	}
		}
    	if (group.getGroupBindingNames().isEmpty()) {
    		// no GROUP BY present
    		return (parent, bindings) -> {
	    		step.evaluate(new BindingSetPipe(parentStrategy.track(parent, group)) {
	    			final GroupValue aggregators = createGroupValue(group, opArgSteps, bindings, evalContext);
					@Override
					protected boolean next(BindingSet bs) {
						if (parent.isClosed()) {
							return false;
						}
						aggregators.addValues(bs);
						return true;
					}
					@Override
					protected void doClose() {
						QueryBindingSet result = new QueryBindingSet(bindings);
						aggregators.bindResult(result);
						parent.pushLast(result);
						aggregators.close();
					}
					@Override
					public String toString() {
						return "AggregateBindingSetPipe-noGroupBy";
					}
	    		}, bindings);
    		};
    	} else {
    		return (parent, bindings) -> {
	    		step.evaluate(new BindingSetPipe(parentStrategy.track(parent, group)) {
	    			final Map<BindingSetValues,GroupValue> groupByMap = new ConcurrentHashMap<>();
	    			final String[] groupNames = toStringArray(group.getGroupBindingNames());
					@Override
					protected boolean next(BindingSet bs) {
						GroupValue aggregators = groupByMap.computeIfAbsent(BindingSetValues.create(groupNames, bs), k -> createGroupValue(group, opArgSteps, bindings, evalContext));
						aggregators.addValues(bs);
						return true;
					}
					@Override
					protected void doClose() {
						if (!groupByMap.isEmpty()) {
							for(Map.Entry<BindingSetValues,GroupValue> aggEntry : groupByMap.entrySet()) {
								BindingSetValues groupKey = aggEntry.getKey();
								MutableBindingSet result = groupKey.setBindings(groupNames, bindings);
								try (GroupValue aggregators = aggEntry.getValue()) {
									aggregators.bindResult(result);
								}
								if (!parent.push(result)) {
									break;
								}
							}
						} else {
							QueryBindingSet result = new QueryBindingSet(bindings);
							for (int i=0; i<opArgSteps.length; i++) {
								GroupElem ge = elems.get(i);
								Aggregator<?,?> agg = createAggregator(ge.getOperator(), opArgSteps[i], bindings, evalContext);
								if (agg != null) {
									try {
										Value v = agg.getValue();
										if (v != null) {
											result.setBinding(ge.getName(), v);
										}
									} catch (ValueExprEvaluationException ignore) {
										// There was a type error when calculating the value of the aggregate. We silently ignore the error,
										// resulting in no result value being bound.
									} finally {
										agg.close();
									}
								}
							}
							if (result.size() > 0) {
								parent.push(result);
							}
						}
						parent.close();
					}
					@Override
					public String toString() {
						return "AggregateBindingSetPipe";
					}
	    		}, bindings);
    		};
    	}
    }

    private GroupValue createGroupValue(Group group, ValuePipeQueryValueEvaluationStep[] opArgSteps, BindingSet parentBindings, QueryEvaluationContext evalContext) {
		Map<String,Aggregator<?,?>> aggregators = new HashMap<>();
        List<GroupElem> elems = group.getGroupElements();
		for (int i=0; i<opArgSteps.length; i++) {
			GroupElem ge = elems.get(i);
			Aggregator<?,?> agg = createAggregator(ge.getOperator(), opArgSteps[i], parentBindings, evalContext);
			if (agg != null) {
				aggregators.put(ge.getName(), agg);
			}
		}
		return new GroupValue(aggregators);
    }

    private static final class GroupValue implements AutoCloseable {
		private final Map<String, Aggregator<?,?>> aggregators;

		GroupValue(Map<String, Aggregator<?,?>> aggregators) {
			this.aggregators = aggregators;
		}

		void addValues(BindingSet bindingSet) {
			for(Aggregator<?,?> agg : aggregators.values()) {
				agg.process(bindingSet);
			}
		}

		void bindResult(MutableBindingSet bs) {
			for(Map.Entry<String,Aggregator<?,?>> entry : aggregators.entrySet()) {
				try(Aggregator<?,?> agg = entry.getValue()) {
					Value v = agg.getValue();
					if (v != null) {
						bs.setBinding(entry.getKey(), v);
					}
				} catch (ValueExprEvaluationException ignore) {
					// There was a type error when calculating the value of the
					// aggregate.
					// We silently ignore the error, resulting in no result value
					// being bound.
				}
			}
		}

		@Override
		public void close() {
			for (Aggregator<?,?> agg : aggregators.values()) {
				agg.close();
			}
		}

		@Override
		public String toString() {
			return aggregators.toString();
		}
    }

	private static final Predicate<?> ALWAYS_TRUE = (v) -> true;

	private Aggregator<?,?> createAggregator(AggregateOperator operator, ValuePipeQueryValueEvaluationStep opArgStep, BindingSet parentBindings, QueryEvaluationContext evalContext) {
		ValueFactory valueFactory = tripleSource.getValueFactory();
		boolean isDistinct = operator.isDistinct();
		if (operator instanceof Count) {
			Count count = (Count) operator;
			if (count.getArg() != null) {
				QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
				Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
				return Aggregator.create(new CountAggregateFunction(eval), distinct, new LongCollector(valueFactory));
			} else {
				Predicate<BindingSet> distinct = isDistinct ? createDistinctBindingSets() : (Predicate<BindingSet>) ALWAYS_TRUE;
				return Aggregator.create(new WildcardCountAggregateFunction(), distinct, new LongCollector(valueFactory));
			}
		} else if (operator instanceof Min) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new MinAggregateFunction(eval), distinct, ValueCollector.create(parentStrategy.isStrict()));
		} else if (operator instanceof Max) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new MaxAggregateFunction(eval), distinct, ValueCollector.create(parentStrategy.isStrict()));
		} else if (operator instanceof Sum) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new SumAggregateFunction(eval), distinct, new NumberCollector());
		} else if (operator instanceof Avg) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new AvgAggregateFunction(eval), distinct, new AvgCollector(valueFactory));
		} else if (operator instanceof Sample) {
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = (Predicate<Value>) ALWAYS_TRUE;
			return Aggregator.create(new SampleAggregateFunction(eval), distinct, new SampleCollector());
		} else if (operator instanceof GroupConcat) {
			GroupConcat grpConcat = (GroupConcat) operator;
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			String sep;
			ValueExpr sepExpr = grpConcat.getSeparator();
			if (sepExpr != null) {
				sep = parentStrategy.precompile(sepExpr, evalContext).evaluate(parentBindings).stringValue();
			} else {
				sep = " ";
			}
			return Aggregator.create(new ConcatAggregateFunction(eval), distinct, new CSVCollector(sep, valueFactory));
		} else if (operator instanceof AggregateFunctionCall) {
			AggregateFunctionCall aggFuncCall = (AggregateFunctionCall) operator;
			AggregateFunctionFactory aggFuncFactory = parentStrategy.aggregateFunctionRegistry.get(aggFuncCall.getIRI())
				.orElseThrow(() -> new QueryEvaluationException("Unknown aggregate function '" + aggFuncCall.getIRI() + "'"));
			QueryValueStepEvaluator eval = new QueryValueStepEvaluator(opArgStep);
			AggregateFunction aggFunc = aggFuncFactory.buildFunction(eval);
			Predicate<Value> distinct = isDistinct ? createDistinctValues() : (Predicate<Value>) ALWAYS_TRUE;
			if (aggFunc instanceof ThreadSafeAggregateFunction) {
				return Aggregator.create((ThreadSafeAggregateFunction) aggFunc, distinct, aggFuncFactory.getCollector());
			} else {
				return SynchronizedAggregator.create(aggFunc, distinct, aggFuncFactory.getCollector());
			}
		} else {
			return null;
		}
    }

	private DistinctValues createDistinctValues() {
		return new DistinctValues(collectionMemoryThreshold);
	}

	private DistinctBindingSets createDistinctBindingSets() {
		return new DistinctBindingSets(collectionMemoryThreshold);
	}

	private static class Aggregator<T extends AggregateCollector, D> implements AutoCloseable {
    	private final Predicate<D> distinctPredicate;
    	private final AggregateFunction<T, D> aggFunc;
    	private final T valueCollector;

    	static <T extends AggregateCollector,D> Aggregator<T,D> create(ThreadSafeAggregateFunction<T,D> aggFunc, Predicate<D> distinctPredicate, T valueCollector) {
    		return new Aggregator<T,D>(aggFunc, distinctPredicate, valueCollector);
    	}

    	private Aggregator(AggregateFunction<T, D> aggFunc, Predicate<D> distinctPredicate, T valueCollector) {
			this.distinctPredicate = distinctPredicate;
			this.aggFunc = aggFunc;
			this.valueCollector = valueCollector;
		}

    	void process(BindingSet bs) {
    		aggFunc.processAggregate(bs, distinctPredicate, valueCollector);
    	}

    	Value getValue() {
    		return valueCollector.getFinalValue();
    	}

    	@Override
		public void close() {
			if (distinctPredicate instanceof AutoCloseable) {
				try {
					((AutoCloseable)distinctPredicate).close();
				} catch (Exception ignore) {
				}
			}
		}
    }

    private static class SynchronizedAggregator<T extends AggregateCollector, D> extends Aggregator<T, D> {
    	static <T extends AggregateCollector,D> SynchronizedAggregator<T,D> create(AggregateFunction<T,D> aggFunc, Predicate<D> distinctPredicate, T valueCollector) {
    		return new SynchronizedAggregator<T,D>(aggFunc, distinctPredicate, valueCollector);
    	}

    	private SynchronizedAggregator(AggregateFunction<T, D> aggFunc, Predicate<D> distinctPredicate, T valueCollector) {
    		super(aggFunc, distinctPredicate, valueCollector);
		}

    	@Override
    	void process(BindingSet bs) {
    		synchronized (this) {
    			super.process(bs);
    		}
    	}

    	@Override
    	Value getValue() {
    		synchronized (this) {
    			return super.getValue();
    		}
    	}
    }

	private static class DistinctValues implements Predicate<Value>, AutoCloseable {
		private final int threshold;
		private BigHashSet<Value> distinctValues;

		DistinctValues(int threshold) {
			this.threshold = threshold;
		}

		@Override
		public boolean test(Value v) {
			if (distinctValues == null) {
				distinctValues = BigHashSet.create(threshold);
			}
			try {
				return distinctValues.add(v);
			} catch (IOException e) {
				throw new QueryEvaluationException(e);
			}
		}

		@Override
		public void close() {
			if (distinctValues != null) {
				distinctValues.close();
				distinctValues = null;
			}
		}
	}

	private static class DistinctBindingSets implements Predicate<BindingSet>, AutoCloseable {
		private final int threshold;
		private BigHashSet<BindingSet> distinctBindingSets;

		DistinctBindingSets(int threshold) {
			this.threshold = threshold;
		}

		@Override
		public boolean test(BindingSet v) {
			if (distinctBindingSets == null) {
				distinctBindingSets = BigHashSet.create(threshold);
			}
			try {
				return distinctBindingSets.add(v);
			} catch (IOException e) {
				throw new QueryEvaluationException(e);
			}
		}

		@Override
		public void close() {
			if (distinctBindingSets != null) {
				distinctBindingSets.close();
				distinctBindingSets = null;
			}
		}
	}

    /**
     * Precompile {@link Reduced} query model nodes
     * @param reduced
     */
    private BindingSetPipeEvaluationStep precompileReduced(Reduced reduced, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(reduced.getArg(), evalContext);
        return (parent, bindings) -> {
	        step.evaluate(new BindingSetPipe(parentStrategy.track(parent, reduced)) {
	            private final AtomicReference<BindingSet> previous = new AtomicReference<>();
	
	            @Override
	            protected boolean next(BindingSet bs) {
	            	BindingSet localPrev = previous.get();
                    if (bs.equals(localPrev)) {
                        return true;
                    }
                    previous.compareAndSet(localPrev, bs);
	                return parent.push(bs);
	            }
	            @Override
	            public String toString() {
	            	return "ReducedBindingSetPipe";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompile {@link Distinct} query model nodes
     * @param distinct
     */
    private BindingSetPipeEvaluationStep precompileDistinct(final Distinct distinct, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep step = precompileTupleExpr(distinct.getArg(), evalContext);
        return (parent, bindings) -> {
	        step.evaluate(new BindingSetPipe(parentStrategy.track(parent, distinct)) {
	            private final BigHashSet<BindingSet> set = BigHashSet.create(collectionMemoryThreshold);
	            @Override
	            public boolean handleException(Throwable e) {
	                set.close();
	                return parent.handleException(e);
	            }
	            @Override
	            protected boolean next(BindingSet bs) {
	                try {
	                    if (!set.add(bs)) {
	                        return true;
	                    }
	                } catch (IOException e) {
	                    return handleException(e);
	                }
	                return parent.push(bs);
	            }
	            @Override
				protected void doClose() {
	               	set.close();
	                parent.close();
	            }
	            @Override
	            public String toString() {
	            	return "DistinctBindingSetPipe";
	            }
	        }, bindings);
        };
    }

	/**
	 * Precompile {@link Extension} query model nodes
	 * @param extension
	 */
    private BindingSetPipeEvaluationStep precompileExtension(final Extension extension, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep argStep = precompileTupleExpr(extension.getArg(), evalContext);
        List<ExtensionElem> extElems = extension.getElements();
        List<org.apache.commons.lang3.tuple.Triple<String,ValuePipeQueryValueEvaluationStep,QueryEvaluationException>> nonAggs = new ArrayList<>(extElems.size());
        for (ExtensionElem extElem : extElems) {
        	ValueExpr expr = extElem.getExpr();
        	if (!(expr instanceof AggregateOperator)) {
        		ValuePipeQueryValueEvaluationStep elemStep;
        		QueryEvaluationException ex;
        		try {
        			elemStep = parentStrategy.precompile(expr, evalContext);
        			ex = null;
        		} catch (QueryEvaluationException e) {
        			elemStep = null;
        			ex = e;
        		}
        		nonAggs.add(org.apache.commons.lang3.tuple.Triple.of(extElem.getName(), elemStep, ex));
        	}
        }

        return (parent, bindings) -> {
	        argStep.evaluate(new BindingSetPipe(parent) {
	            @Override
	            protected boolean next(BindingSet bs) {
	                QueryBindingSet targetBindings = new QueryBindingSet(bs);
	                for (org.apache.commons.lang3.tuple.Triple<String,ValuePipeQueryValueEvaluationStep,QueryEvaluationException> nonAgg : nonAggs) {
	                	QueryEvaluationException ex = nonAgg.getRight();
	                	if (ex != null) {
	                		return handleException(ex);
	                	}
                		String extElemName = nonAgg.getLeft();
                		ValuePipeQueryValueEvaluationStep elemStep = nonAgg.getMiddle();
                        try {
                            // we evaluate each extension element over the targetbindings, so that bindings from
                            // a previous extension element in this same extension can be used by other extension elements.
                            // e.g. if a projection contains (?a + ?b as ?c) (?c * 2 as ?d)
                            Value targetValue = elemStep.evaluate(targetBindings);
                            if (targetValue != null) {
                                // Potentially overwrites bindings from super
                                targetBindings.setBinding(extElemName, targetValue);
                            }
                        } catch (ValueExprEvaluationException e) {
                            // silently ignore type errors in extension arguments. They should not cause the
                            // query to fail but result in no bindings for this solution
                            // see https://www.w3.org/TR/sparql11-query/#assignment
                            // use null as place holder for unbound variables that must remain so
                            targetBindings.setBinding(extElemName, null);
                        } catch (QueryEvaluationException e) {
                            return handleException(e);
                        }
	                }
	                return parent.push(targetBindings);
	            }
	            @Override
	            public String toString() {
	            	return "ExtensionBindingSetPipe";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompile {@link Slice} query model nodes.
     * @param slice
     */
    private BindingSetPipeEvaluationStep precompileSlice(Slice slice, QueryEvaluationContext evalContext) {
        final long offset = slice.hasOffset() ? slice.getOffset() : 0;
        final long limit = slice.hasLimit() ? offset + slice.getLimit() : Long.MAX_VALUE;
        BindingSetPipeEvaluationStep step = precompileTupleExpr(slice.getArg(), evalContext);
        return (parent, bindings) -> {
	        step.evaluate(new BindingSetPipe(parentStrategy.track(parent, slice)) {
	            private final AtomicLong counter = new AtomicLong(0);
	            private final AtomicLong remaining = new AtomicLong(limit-offset);
	            @Override
	            protected boolean next(BindingSet bs) {
	                long l = counter.incrementAndGet();
	                if (l <= offset) {
	                    return !parent.isClosed();
	                } else if (l <= limit) {
		            	boolean pushMore = parent.push(bs);
		            	if (remaining.decrementAndGet() == 0) {
		            		// we're the last one so close
		            		close();
		            		pushMore = false;
		            	}
		            	return pushMore;
	                } else {
	                	return false;
	                }
	            }
	            @Override
	            public String toString() {
	            	return "SliceBindingSetPipe";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompiles {@link Service} query model nodes
     * @param service
     */
    private BindingSetPipeEvaluationStep precompileService(Service service) {
        Var serviceRef = service.getServiceRef();
        Value serviceRefValue = serviceRef.getValue();
        if (serviceRefValue != null) {
        	// service endpoint known ahead of time
        	FederatedService fs = parentStrategy.getService(serviceRefValue.stringValue());
        	return (topPipe, bindings) -> evaluateService(topPipe, service, fs, bindings);
        } else {
			return (topPipe, bindings) -> {
				Value boundServiceRefValue = bindings.getValue(serviceRef.getName());
			    if (boundServiceRefValue != null) {
			        String serviceUri = boundServiceRefValue.stringValue();
		        	FederatedService fs = parentStrategy.getService(serviceUri);
					evaluateService(topPipe, service, fs, bindings);
			    } else {
			        topPipe.handleException(new QueryEvaluationException("SERVICE variables must be bound at evaluation time."));
			    }
			};
        }
    }

    private void evaluateService(BindingSetPipe topPipe, Service service, FederatedService fs, BindingSet bindings) {
        // create a copy of the free variables, and remove those for which
        // bindings are available (we can set them as constraints!)
        Set<String> freeVars = new HashSet<>(service.getServiceVars());
        freeVars.removeAll(bindings.getBindingNames());
        // Get bindings from values pre-bound into variables.
        MapBindingSet fsBindings = new MapBindingSet(bindings.size() + 1);
        for (Binding binding : bindings) {
            fsBindings.addBinding(binding.getName(), binding.getValue());
        }
        final Set<Var> boundVars = new HashSet<Var>();
        new AbstractExtendedQueryModelVisitor<RuntimeException> (){
            @Override
            public void meet(Var var) {
                if (var.hasValue()) {
                    boundVars.add(var);
                }
            }
        }.meet(service);
        for (Var boundVar : boundVars) {
            freeVars.remove(boundVar.getName());
            fsBindings.addBinding(boundVar.getName(), boundVar.getValue());
        }

        String baseUri = service.getBaseURI();
		BindingSetPipe pipe = parentStrategy.track(topPipe, service);
	    try {
	        // special case: no free variables => perform ASK query
	        if (freeVars.isEmpty()) {
	            // check if triples are available (with inserted bindings)
	            if (fs.ask(service, fsBindings, baseUri)) {
	                pipe.push(fsBindings);
	            }
            	pipe.close();
	        } else {
		        // otherwise: perform a SELECT query
		        if (fs instanceof BindingSetPipeFederatedService) {
            		ValueFactory cachingVF = new CachingValueFactory(tripleSource.getValueFactory(), valueCacheSize);
					((BindingSetPipeFederatedService)fs).select(new BindingSetPipe(pipe) {
						@Override
						protected boolean next(BindingSet theirBs) {
							// in same VM so need to explicitly convert Values
							BindingSet ourBs = ValueFactories.convertValues(theirBs, cachingVF);
							return parent.push(ourBs);
						}
						@Override
						public boolean handleException(Throwable e) {
							if (service.isSilent()) {
								parent.pushLast(fsBindings);
							} else {
								super.handleException(e);
							}
							return false;
						}
					}, service, freeVars, fsBindings, baseUri);
		        } else if (fs instanceof BindingSetConsumerFederatedService) {
            		ValueFactory cachingVF = new CachingValueFactory(tripleSource.getValueFactory(), valueCacheSize);
	            	try {
	            		((BindingSetConsumerFederatedService)fs).select(theirBs -> {
	            			// in same VM so need to explicitly convert Values
	            			BindingSet ourBs = ValueFactories.convertValues(theirBs, cachingVF);
	            			if(!pipe.push(ourBs)) {
	            				AbortConsumerException.abort();
	            			}
	            		}, service, freeVars, fsBindings, baseUri);
            		} catch (AbortConsumerException abort) {
            			// ignore
	            	}
            		pipe.close();
		        } else {
		            QueryEvaluationStep evalStep = bs -> {
		            	try {
		            		return fs.select(service, freeVars, bs, baseUri);
		            	} catch (RuntimeException e) {
		            		if (service.isSilent()) {
		            			return new SingletonIteration<>(fsBindings);
		            		} else {
		            			throw e;
		            		}
		            	}
	            	};
		            executor.pullAndPushAsync(topPipe, evalStep, service, fsBindings, parentStrategy);
		        }
	        }
	    } catch (Throwable e) {
	        if (service.isSilent()) {
	            pipe.pushLast(fsBindings);
	        } else {
	            pipe.handleException(e);
	        }
	    }
    }

    /**
     * Precompiles {@link BinaryTupleOperator} query model nodes
     */
    private BindingSetPipeEvaluationStep precompileBinaryTupleOperator(BinaryTupleOperator expr, QueryEvaluationContext evalContext) {
        if (expr instanceof Join) {
            return precompileJoin((Join) expr, evalContext);
        } else if (expr instanceof LeftJoin) {
        	return precompileLeftJoin((LeftJoin) expr, evalContext);
        } else if (expr instanceof Union) {
        	return precompileUnion((Union) expr, evalContext);
        } else if (expr instanceof Intersection) {
        	return precompileIntersection((Intersection) expr, evalContext);
        } else if (expr instanceof Difference) {
        	return precompileDifference((Difference) expr, evalContext);
        } else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        } else {
            throw new QueryEvaluationException("Unsupported binary tuple operator type: " + expr.getClass());
        }
    }

    /**
     * Precompiles {@link Join} query model nodes.
     */
    private BindingSetPipeEvaluationStep precompileJoin(Join join, QueryEvaluationContext evalContext) {
    	BindingSetPipeEvaluationStep step;
    	String algorithm = join.getAlgorithmName();
    	if (isOutOfScopeForLeftArgBindings(join.getRightArg())) {
    		algorithm = Algorithms.HASH_JOIN;
    	}

    	if (Algorithms.HASH_JOIN.equals(algorithm)) {
    		step = new HashJoinEvaluationStep(join, evalContext);
    	} else {
    		step = precompileNestedLoopsJoin(join, evalContext);
    	}
    	return step;
    }

    private static boolean isOutOfScopeForLeftArgBindings(TupleExpr expr) {
		return (TupleExprs.isVariableScopeChange(expr) || TupleExprs.containsSubquery(expr));
	}

    private BindingSetPipeEvaluationStep precompileNestedLoopsJoin(Join join, QueryEvaluationContext evalContext) {
    	join.setAlgorithm(Algorithms.NESTED_LOOPS);
        BindingSetPipeEvaluationStep outerStep = precompileTupleExpr(join.getLeftArg(), evalContext);
        BindingSetPipeEvaluationStep innerStep = precompileTupleExpr(join.getRightArg(), evalContext);
        return precompileNestedLoopsJoin(outerStep, innerStep, join);
    }

    private BindingSetPipeEvaluationStep precompileNestedLoopsJoin(BindingSetPipeEvaluationStep outerStep, BindingSetPipeEvaluationStep innerStep, TupleExpr trackExpr) {
        return (topPipe, bindings) -> {
        	if (trackExpr != null) {
        		topPipe = parentStrategy.track(topPipe, trackExpr);
        	}
	        outerStep.evaluate(new PipeJoin(topPipe) {
	            @Override
	            protected boolean next(BindingSet bs) {
	            	startSecondaryPipe();
	                innerStep.evaluate(new BindingSetPipe(parent) {
	                	@Override
	                	protected boolean next(BindingSet bs) {
	                		return pushToParent(bs);
	                	}
	                    @Override
	    				protected void doClose() {
	                    	endSecondaryPipe();
	                    }
	                    @Override
	                    public String toString() {
	                    	return "JoinBindingSetPipe(inner)";
	                    }
	                }, bs);
	                return !parent.isClosed(); // innerStep is async, check if we've been closed
	            }
	            @Override
	            public String toString() {
	            	return "JoinBindingSetPipe(outer)";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompiles {@link LeftJoin} query model nodes
     */
    private BindingSetPipeEvaluationStep precompileLeftJoin(LeftJoin leftJoin, QueryEvaluationContext evalContext) {
    	BindingSetPipeEvaluationStep step;
    	String algorithm = leftJoin.getAlgorithmName();
    	if (TupleExprs.containsSubquery(leftJoin.getRightArg())) {
    		algorithm = Algorithms.HASH_JOIN;
    	}

    	if (Algorithms.HASH_JOIN.equals(algorithm)) {
    		step = new LeftHashJoinEvaluationStep(leftJoin, evalContext);
    	} else {
    		step = precompileNestedLoopsLeftJoin(leftJoin, evalContext);
    	}
    	return step;
    }

    private static QueryBindingSet getFilteredBindings(BindingSet bindings, Set<String> problemVars) {
        QueryBindingSet filteredBindings = new QueryBindingSet(bindings);
        filteredBindings.removeAll(problemVars);
        return filteredBindings;
    }

    private BindingSetPipeEvaluationStep precompileNestedLoopsLeftJoin(LeftJoin leftJoin, QueryEvaluationContext evalContext) {
    	leftJoin.setAlgorithm(Algorithms.NESTED_LOOPS);
        BindingSetPipeEvaluationStep leftStep = precompileTupleExpr(leftJoin.getLeftArg(), evalContext);
        BindingSetPipeEvaluationStep rightStep = precompileTupleExpr(leftJoin.getRightArg(), evalContext);
        ValuePipeQueryValueEvaluationStep conditionStep = leftJoin.hasCondition() ? parentStrategy.precompile(leftJoin.getCondition(), evalContext) : null;
    	return (parentPipe, bindings) -> {
	        parentPipe = parentStrategy.track(parentPipe, leftJoin);
	    	// Check whether optional join is "well designed" as defined in section
	        // 4.2 of "Semantics and Complexity of SPARQL", 2006, Jorge Pérez et al.
	        VarNameCollector optionalVarCollector = new VarNameCollector();
	        leftJoin.getRightArg().visit(optionalVarCollector);
	        if (leftJoin.hasCondition()) {
	            leftJoin.getCondition().visit(optionalVarCollector);
	        }
	        final Set<String> problemVars = new HashSet<>(optionalVarCollector.getVarNames());
	        problemVars.removeAll(leftJoin.getLeftArg().getBindingNames());
	        problemVars.retainAll(bindings.getBindingNames());
	        final Set<String> scopeBindingNames = leftJoin.getBindingNames();
	        if (!problemVars.isEmpty()) {
	        	parentPipe = new BindingSetPipe(parentPipe) {
		            //Handle badly designed left join
		            @Override
		            protected boolean next(BindingSet bs) {
		            	if (QueryResults.bindingSetsCompatible(bindings, bs)) {
		                    // Make sure the provided problemVars are part of the returned results
		                    // (necessary in case of e.g. LeftJoin and Union arguments)
		                    QueryBindingSet extendedResult = null;
		                    for (String problemVar : problemVars) {
		                        if (!bs.hasBinding(problemVar)) {
		                            if (extendedResult == null) {
		                                extendedResult = new QueryBindingSet(bs);
		                            }
		                            extendedResult.addBinding(problemVar, bindings.getValue(problemVar));
		                        }
		                    }
		                    if (extendedResult != null) {
		                        bs = extendedResult;
		                    }
		                    return parent.push(bs);
		            	} else {
		            		return true;
		            	}
		            }
		            @Override
		            public String toString() {
		            	return "LeftJoinBindingSetPipe(top)";
		            }
		        };
	        }
	        leftStep.evaluate(new PipeJoin(parentPipe) {
	        	@Override
	            protected boolean next(final BindingSet leftBindings) {
	        		startSecondaryPipe();
	                rightStep.evaluate(new BindingSetPipe(parent) {
	                    private boolean failed = true;
	                    @Override
	                    protected boolean next(BindingSet rightBindings) {
	                    	try {
	                            if (conditionStep == null) {
	                                failed = false;
	                                return pushToParent(rightBindings);
	                            } else {
	                                // Limit the bindings to the ones that are in scope for
	                                // this filter
	                                QueryBindingSet scopeBindings = new QueryBindingSet(rightBindings);
	                                scopeBindings.retainAll(scopeBindingNames);
	                                if (parentStrategy.isTrue(conditionStep, scopeBindings)) {
	                                    failed = false;
	                                    return pushToParent(rightBindings);
	                                }
	                            }
	                        } catch (ValueExprEvaluationException ignore) {
	                        } catch (QueryEvaluationException e) {
	                            return handleException(e);
	                        }
	                        return true;
	                    }
	                    @Override
	    				protected void doClose() {
	                        if (failed) {
	                            // Join failed, return left arg's bindings
	                        	pushToParent(leftBindings);
	                        }
	                        endSecondaryPipe();
	                    }
	                    @Override
	                    public String toString() {
	                    	return "LeftJoinBindingSetPipe(right)";
	                    }
	                }, leftBindings);
	                return !parent.isClosed(); // rightStep is async, check if we've been closed
	            }
	            @Override
	            public String toString() {
	            	return "LeftJoinBindingSetPipe(left)";
	            }
	        }, problemVars.isEmpty() ? bindings : getFilteredBindings(bindings, problemVars));
    	};
    }


	private static boolean isSameConstant(Var v1, Var v2) {
		return v1.isConstant() && v2.isConstant() && v1.getValue().equals(v2.getValue());
	}

	private static Map<String,Var> getVars(StatementPattern sp) {
		Map<String,Var> vars = new HashMap<>(5);
		Var subjVar = sp.getSubjectVar();
		vars.put(subjVar.getName(), subjVar);
		Var predVar = sp.getPredicateVar();
		vars.put(predVar.getName(), predVar);
		Var objVar = sp.getObjectVar();
		vars.put(objVar.getName(), objVar);
		Var ctxVar = sp.getContextVar();
		if (ctxVar != null) {
			vars.put(ctxVar.getName(), ctxVar);
		}
		return vars;
	}

    private static Set<String> getUnboundNames(Collection<String> names, BindingSet bindings) {
    	Set<String> unbound = new HashSet<>(names.size()+1);
    	for (String name : names) {
    		if (!bindings.hasBinding(name)) {
    			unbound.add(name);
    		}
    	}
    	return unbound;
    }

    private static List<String> getUnboundNames(Collection<String> names, Set<String> requiredNames, BindingSet bindings) {
    	List<String> unbound = new ArrayList<>(names.size());
    	unbound.addAll(requiredNames);
    	for (String name : names) {
    		if (!requiredNames.contains(name) && !bindings.hasBinding(name)) {
    			unbound.add(name);
    		}
    	}
    	return unbound;
    }

    private static <E> String[] toStringArray(Collection<E> c) {
        return c.toArray(new String[c.size()]);
    }

    private abstract class AbstractHashJoinEvaluationStep implements BindingSetPipeEvaluationStep {
    	private final BinaryTupleOperator join;
    	private final TupleExpr probeExpr;
    	private final TupleExpr buildExpr;
    	private final BindingSetPipeEvaluationStep probeStep;
    	private final BindingSetPipeEvaluationStep buildStep;
    	private final int hashTableLimit;
    	private final Set<String> estimatedJoinBindings;
		private final Set<String> estimatedBuildBindings;
		private final int initialSize;

    	protected AbstractHashJoinEvaluationStep(BinaryTupleOperator join, int hashTableLimit, QueryEvaluationContext evalContext) {
	    	join.setAlgorithm(Algorithms.HASH_JOIN);
    		this.join = join;
        	this.probeExpr = join.getLeftArg();
        	this.buildExpr = join.getRightArg();
        	this.probeStep = precompileTupleExpr(probeExpr, evalContext);
        	this.buildStep = precompileTupleExpr(buildExpr, evalContext);
        	this.hashTableLimit = hashTableLimit;
        	this.estimatedJoinBindings = estimateJoinBindings();
    		this.estimatedBuildBindings = buildExpr.getBindingNames();
    		this.initialSize = (int) Math.min(MAX_INITIAL_HASH_JOIN_TABLE_SIZE, Math.max(0, buildExpr.getResultSizeEstimate()));
    	}

    	private Set<String> estimateJoinBindings() {
    		Set<String> joinAttributeNames;
    		// for statement patterns can ignore equal common constant vars
    		if (probeExpr instanceof StatementPattern && buildExpr instanceof StatementPattern) {
    			Map<String,Var> probeVars = getVars((StatementPattern) probeExpr);
    			Map<String,Var> buildVars = getVars((StatementPattern) buildExpr);
    			joinAttributeNames = new HashSet<>(5);
    			for (Map.Entry<String,Var> entry : probeVars.entrySet()) {
    				String name = entry.getKey();
    				Var probeVar = entry.getValue();
    				Var buildVar = buildVars.get(name);
    				if (buildVar != null) {
    					if (!isSameConstant(probeVar, buildVar)) {
    						joinAttributeNames.add(name);
    					}
    				}
    			}
    		} else {
        		joinAttributeNames = probeExpr.getBindingNames();
        		if (!joinAttributeNames.isEmpty()) {
        			// make modifiable
        			joinAttributeNames = new HashSet<>(joinAttributeNames);
        			joinAttributeNames.retainAll(buildExpr.getBindingNames());
        		}
    		}
    		return joinAttributeNames;
    	}

    	final HashJoinTable createHashTable(Set<String> joinBindings, List<String> buildBindings) {
	    	return new HashJoinTable(initialSize, joinBindings, buildBindings);
    	}

		@Override
		public void evaluate(BindingSetPipe parent, BindingSet bindings) {
			Set<String> actualJoinBindings = getUnboundNames(estimatedJoinBindings, bindings);
			List<String> actualBuildBindings = getUnboundNames(estimatedBuildBindings, actualJoinBindings, bindings);
			buildStep.evaluate(new PipeJoin(parentStrategy.track(parent, join)) {
		    	HashJoinTable hashTable = createHashTable(actualJoinBindings, actualBuildBindings);
	            @Override
	            protected boolean next(BindingSet buildBs) {
					if (parent.isClosed()) {
						return false;
					}
	            	HashJoinTable partition;
	            	synchronized (this) {
	                	if (hashTable.entryCount() >= hashTableLimit) {
	                		partition = hashTable;
	                		hashTable = createHashTable(actualJoinBindings, actualBuildBindings);
	                	} else {
	                		partition = null;
	                	}
	            		hashTable.put(buildBs);
	            	}
	            	if (partition != null) {
	            		startJoin(partition, false);
	            	}
	            	return true;
	            }
	            @Override
				protected void doClose() {
	            	synchronized (this) {
	            		if (hashTable != null) {
	                   		startJoin(hashTable, true);
	            			hashTable = null;
	            		}
	            	}
	            }
	        	/**
	        	 * Performs a hash-join.
	        	 * @param hashTablePartition hash table to join against.
	        	 * @param isLast true if this is the last time doJoin() will be called for the current join operation.
	        	 */
	        	private void startJoin(HashJoinTable hashTablePartition, boolean isLast) {
	        		if (hashTablePartition.entryCount() == 0) {
	        			if (isLast) {
	        				parent.close();
	        			}
	        		} else {
	        			startSecondaryPipe(isLast);
	        			// NB: this part may execute asynchronously
	                	probeStep.evaluate(createPipe(this, hashTablePartition), bindings);
	        		}
	        	}
	            @Override
	            public String toString() {
	            	return "HashTableBindingSetPipe";
	            }
	    	}, bindings);
	    }

    	protected abstract BindingSetPipe createPipe(PipeJoin primary, HashJoinTable hashTablePartition);

    	abstract class AbstractHashJoinBindingSetPipe extends BindingSetPipe {
    		private final PipeJoin primary;
    		protected final HashJoinTable hashTablePartition;

    		protected AbstractHashJoinBindingSetPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
				super(primary.getParent());
				this.primary = primary;
				this.hashTablePartition = hashTablePartition;
			}
			protected final boolean pushToParent(BindingSet bs) {
        		return primary.pushToParent(bs);
        	}
			boolean hasUnboundOptionalValue(BindingSet probeBs) {
		    	for (String name : hashTablePartition.joinBindings) {
		    		if (probeBs.getValue(name) == null) {
		    			return true;
		    		}
		    	}
		    	return false;
			}
			BindingSet join(BindingSetValues buildBsv, BindingSet probeBs) {
				return buildBsv.joinTo(hashTablePartition.buildBindings, probeBs);
			}
			BindingSet tryJoin(BindingSetValues buildBsv, BindingSet probeBs) {
				return buildBsv.tryJoin(hashTablePartition.buildBindings, hashTablePartition.joinKeySet, probeBs);
			}

			@Override
			protected void doClose() {
            	primary.endSecondaryPipe();
        	}
    	}
    }

	final class HashJoinEvaluationStep extends AbstractHashJoinEvaluationStep {
		HashJoinEvaluationStep(Join join, QueryEvaluationContext evalContext) {
			super(join, hashJoinLimit, evalContext);
		}

		final class HashJoinBindingSetPipe extends AbstractHashJoinBindingSetPipe {
			protected HashJoinBindingSetPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
				super(primary, hashTablePartition);
			}
			@Override
			protected boolean next(BindingSet probeBs) {
				if (probeBs.isEmpty()) {
					// the empty binding set should be merged with all binding sets in the hash table
					Collection<? extends List<BindingSetValues>> hashValues = hashTablePartition.all();
					for (List<BindingSetValues> hashValue : hashValues) {
						for (BindingSetValues bsv : hashValue) {
							if (!pushToParent(join(bsv, probeBs))) {
								return false;
							}
						}
					}
				} else if (hasUnboundOptionalValue(probeBs)) {
					// have to manually search through all the binding sets
					Collection<? extends List<BindingSetValues>> hashValues = hashTablePartition.all();
					for (List<BindingSetValues> hashValue : hashValues) {
						for (BindingSetValues bsv : hashValue) {
							BindingSet bs = tryJoin(bsv, probeBs);
							if (bs != null) {
								if (!pushToParent(bs)) {
									return false;
								}
							}
						}
					}
				} else {
					List<BindingSetValues> hashValue = hashTablePartition.get(probeBs);
					if (hashValue != null && !hashValue.isEmpty()) {
						for (BindingSetValues bsv : hashValue) {
							if (!pushToParent(join(bsv, probeBs))) {
								return false;
							}
						}
					}
				}
				return !parent.isClosed();
			}
		    @Override
		    public String toString() {
		    	return "HashJoinBindingSetPipe";
		    }
		}

		@Override
		protected BindingSetPipe createPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
			return new HashJoinBindingSetPipe(primary, hashTablePartition);
		}
	}

	final class LeftHashJoinEvaluationStep extends AbstractHashJoinEvaluationStep {
		LeftHashJoinEvaluationStep(LeftJoin join, QueryEvaluationContext evalContext) {
			super(join, Integer.MAX_VALUE, evalContext);
		}

		final class LeftHashJoinBindingSetPipe extends AbstractHashJoinBindingSetPipe {
			protected LeftHashJoinBindingSetPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
				super(primary, hashTablePartition);
			}
			@Override
			protected boolean next(BindingSet probeBs) {
				if (probeBs.isEmpty()) {
					// the empty binding set should be merged with all binding sets in the hash table
					Collection<? extends List<BindingSetValues>> hashValues = hashTablePartition.all();
					for (List<BindingSetValues> hashValue : hashValues) {
						for (BindingSetValues bsv : hashValue) {
							if (!pushToParent(join(bsv, probeBs))) {
								return false;
							}
						}
					}
				} else if (hasUnboundOptionalValue(probeBs)) {
					// have to manually search through all the binding sets
					Collection<? extends List<BindingSetValues>> hashValues = hashTablePartition.all();
					boolean foundJoin = false;
					for (List<BindingSetValues> hashValue : hashValues) {
						for (BindingSetValues bsv : hashValue) {
							BindingSet bs = tryJoin(bsv, probeBs);
							if (bs != null) {
								foundJoin = true;
								if (!pushToParent(bs)) {
									return false;
								}
							}
						}
					}
					if (!foundJoin) {
						if (!pushToParent(probeBs)) {
							return false;
						}
					}
				} else {
					List<BindingSetValues> hashValue = hashTablePartition.get(probeBs);
					if (hashValue != null && !hashValue.isEmpty()) {
						for (BindingSetValues bsv : hashValue) {
							if (!pushToParent(join(bsv, probeBs))) {
								return false;
							}
						}
					} else {
						if (!pushToParent(probeBs)) {
							return false;
						}
					}
				}
				return !parent.isClosed();
			}
		    @Override
		    public String toString() {
		    	return "LeftHashJoinBindingSetPipe";
		    }
		}

		@Override
		protected BindingSetPipe createPipe(PipeJoin primary, HashJoinTable hashTablePartition) {
			return new LeftHashJoinBindingSetPipe(primary, hashTablePartition);
		}
	}

    private static final class HashJoinTable {
    	private final Set<String> joinKeySet;
    	private final String[] joinBindings;
    	private final String[] buildBindings;
    	private final Map<BindingSetValues, List<BindingSetValues>> hashTable;
		private int keyCount;
		private int bsCount;

		HashJoinTable(int initialSize, Set<String> joinBindings, List<String> buildBindings) {
			this.joinKeySet = joinBindings;
			this.joinBindings = joinBindings.toArray(new String[joinBindings.size()]);
			this.buildBindings = buildBindings.toArray(new String[buildBindings.size()]);
    		if (!joinBindings.isEmpty()) {
    			hashTable = new HashMap<>(initialSize);
    		} else {
    			hashTable = Collections.<BindingSetValues, List<BindingSetValues>>singletonMap(BindingSetValues.EMPTY, new ArrayList<>(initialSize));
    		}
    	}

		void put(BindingSet bs) {
			BindingSetValues hashKey = BindingSetValues.create(joinBindings, bs);
			List<BindingSetValues> hashValue = hashTable.get(hashKey);
			boolean newEntry = (hashValue == null);
			if (newEntry) {
				int averageSize = (keyCount > 0) ? (int) (bsCount/keyCount) : 0;
				hashValue = new ArrayList<>(averageSize + 1);
				hashTable.put(hashKey, hashValue);
				keyCount++;
			}
			hashValue.add(BindingSetValues.create(buildBindings, bs));
			bsCount++;
		}

		int entryCount() {
			return bsCount;
		}

    	List<BindingSetValues> get(BindingSet bs) {
    		BindingSetValues key = BindingSetValues.create(joinBindings, bs);
			return hashTable.get(key);
    	}

    	Collection<? extends List<BindingSetValues>> all() {
    		return hashTable.values();
    	}
    }

    /**
     * Precompiles {@link Union} query model nodes.
     * @param union
     */
    private BindingSetPipeEvaluationStep precompileUnion(Union union, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep leftStep = precompileTupleExpr(union.getLeftArg(), evalContext);
        BindingSetPipeEvaluationStep rightStep = precompileTupleExpr(union.getRightArg(), evalContext);
        return (parent, bindings) -> {
    		parent = parentStrategy.track(parent, union);
            final AtomicInteger args = new AtomicInteger(2);
            // A pipe can only be closed once, so need separate instances
	        leftStep.evaluate(new UnionBindingSetPipe(parent, args), bindings);
	        rightStep.evaluate(new UnionBindingSetPipe(parent, args), bindings);
        };
    }

    final class UnionBindingSetPipe extends BindingSetPipe {
        private final AtomicInteger args;

        UnionBindingSetPipe(BindingSetPipe parent, AtomicInteger args) {
    		super(parent);
    		this.args = args;
    	}
        @Override
		protected void doClose() {
        	args.decrementAndGet();
            if (args.compareAndSet(0, -1)) {
                parent.close();
            }
        }
        @Override
        public String toString() {
        	return "UnionBindingSetPipe";
        }
    }

    /**
     * Precompiles {@link Intersection} query model nodes
     * @param intersection
     */
    private BindingSetPipeEvaluationStep precompileIntersection(final Intersection intersection, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep rightStep = precompileTupleExpr(intersection.getRightArg(), evalContext);
        BindingSetPipeEvaluationStep leftStep = precompileTupleExpr(intersection.getLeftArg(), evalContext);
        return (topPipe, bindings) -> {
	        rightStep.evaluate(new BindingSetPipe(topPipe) {
	            private final BigHashSet<BindingSet> secondSet = BigHashSet.create(collectionMemoryThreshold);
	            @Override
	            public boolean handleException(Throwable e) {
	                secondSet.close();
	                return parent.handleException(e);
	            }
	            @Override
	            protected boolean next(BindingSet bs) {
					if (parent.isClosed()) {
						return false;
					}
	                try {
	                    secondSet.add(bs);
	                    return true;
	                } catch (IOException e) {
	                    return handleException(e);
	                }
	            }
	            @Override
				protected void doClose() {
	                leftStep.evaluate(new BindingSetPipe(parent) {
	                    @Override
	                    protected boolean next(BindingSet bs) {
	                        try {
	                            return secondSet.contains(bs) ? parent.push(bs) : true;
	                        } catch (IOException e) {
	                            return handleException(e);
	                        }
	                    }
	                    @Override
	    				protected void doClose() {
	                        secondSet.close();
	                        parent.close();
	                    }
	                    @Override
	                    public String toString() {
	                    	return "IntersectionBindingSetPipe(left)";
	                    }
	                }, bindings);
	            }
	            @Override
	            public String toString() {
	            	return "IntersectionBindingSetPipe(right)";
	            }
	        }, bindings);
        };
    }

    /**
     * Precompiles {@link Difference} query model nodes
     * @param difference
     */
    private BindingSetPipeEvaluationStep precompileDifference(final Difference difference, QueryEvaluationContext evalContext) {
        BindingSetPipeEvaluationStep rightStep = precompileTupleExpr(difference.getRightArg(), evalContext);
        BindingSetPipeEvaluationStep leftStep = precompileTupleExpr(difference.getLeftArg(), evalContext);
        return (topPipe, bindings) -> {
	        rightStep.evaluate(new BindingSetPipe(topPipe) {
	            private final BigHashSet<BindingSet> excludeSet = BigHashSet.create(collectionMemoryThreshold);
	            @Override
	            public boolean handleException(Throwable e) {
	                excludeSet.close();
	                return parent.handleException(e);
	            }
	            @Override
	            protected boolean next(BindingSet bs) {
					if (parent.isClosed()) {
						return false;
					}
	                try {
	                    excludeSet.add(bs);
	                    return true;
	                } catch (IOException e) {
	                    return handleException(e);
	                }
	            }
	            @Override
				protected void doClose() {
	                leftStep.evaluate(new BindingSetPipe(parent) {
	                    @Override
	                    protected boolean next(BindingSet bs) {
	                        for (BindingSet excluded : excludeSet) {
	                            // build set of shared variable names
	                            Set<String> sharedBindingNames = new HashSet<>(excluded.getBindingNames());
	                            sharedBindingNames.retainAll(bs.getBindingNames());
	                            // two bindingsets that share no variables are compatible by
	                            // definition, however, the formal
	                            // definition of SPARQL MINUS indicates that such disjoint sets should
	                            // be filtered out.
	                            // See http://www.w3.org/TR/sparql11-query/#sparqlAlgebra
	                            if (!sharedBindingNames.isEmpty()) {
	                                if (QueryResults.bindingSetsCompatible(excluded, bs)) {
	                                    // at least one compatible bindingset has been found in the
	                                    // exclude set, therefore the object is compatible, therefore it
	                                    // should not be accepted.
	                                    return true;
	                                }
	                            }
	                        }
	                        return parent.push(bs);
	                    }
	                    @Override
	    				protected void doClose() {
	                        excludeSet.close();
	                        parent.close();
	                    }
	                    @Override
	                    public String toString() {
	                    	return "DifferenceBindingSetPipe(left)";
	                    }
	                }, bindings);
	            }
	            @Override
	            public String toString() {
	            	return "DifferenceBindingSetPipe(right)";
	            }
	        }, bindings);
        };
    }

    private BindingSetPipeEvaluationStep precompileStarJoin(StarJoin starJoin, QueryEvaluationContext evalContext) {
    	if (starJoin.getScope() == StatementPattern.Scope.DEFAULT_CONTEXTS && (tripleSource instanceof ExtendedTripleSource)) {
    		ExtendedTripleSource extTripleSource = (ExtendedTripleSource) tripleSource;
	    	StatementPattern[] sps = new StatementPattern[starJoin.getArgCount()];
	    	for (int i=0; i<sps.length; i++) {
	    		TupleExpr te = starJoin.getArg(i);
	    		if (te instanceof StatementPattern) {
	    			sps[i] = (StatementPattern) te;
	    		} else {
	    			break;
	    		}
	    	}
	    	Var commonVar = starJoin.getCommonVar();
	    	Var ctxVar = starJoin.getContextVar();
	    	boolean allAreStmts = (sps[sps.length-1] != null);
	    	if (allAreStmts) {
	    		BindingSetPipeEvaluationStep firstStep = precompileTupleExpr(sps[0], evalContext);
		    	return (parent, bindings) -> {
		        	BindingSetPipeEvaluationStep step;
		        	boolean isCommonBound = commonVar.hasValue() || bindings.hasBinding(commonVar.getName());
		        	boolean isCtxBound = (ctxVar != null) && (ctxVar.hasValue() || bindings.hasBinding(ctxVar.getName()));
		        	boolean isPrebound = isCommonBound && (ctxVar == null || isCtxBound);
		        	int startIndex = isPrebound ? 0 : 1;
	
		        	boolean getFullSubject = false;
		    		IRI[] preds = new IRI[sps.length];
		        	for (int i=0; i<sps.length; i++) {
		       			StatementPattern sp = sps[i];
		        		IRI pred = (IRI) Algebra.getVarValue(sp.getPredicateVar(), bindings);
		        		if (pred == null) {
		        			getFullSubject = true;
		        		}
		        		preds[i] = pred;
		        	}
		        	getFullSubject = true; // TODO currently only support getFullSubject mode
		        	if (getFullSubject) {
		        		// TODO replace this with single evaluateStatemetns if only one sps left
		        		step = (p, stepBindings) -> executor.pullAndPushAsync(p, evalBindings -> {
		        			List<BindingSet>[] resultsPerSp = (List<BindingSet>[]) new List<?>[sps.length];
		    	        	Resource common = (Resource) Algebra.getVarValue(commonVar, evalBindings);
		    	        	Resource ctx = (Resource) Algebra.getVarValue(ctxVar, evalBindings);
		        			Resource[] ctxs = (ctxVar != null) ? new Resource[] {ctx} : ALL_CONTEXTS;
		        			CloseableIteration<? extends Statement, QueryEvaluationException> iter = tripleSource.getStatements(common, null, null, ctxs);
							while (iter.hasNext()) {
								Statement stmt = iter.next();
								for (int i=startIndex; i<sps.length; i++) {
									StatementPattern sp = sps[i];
									Value pred = Algebra.getVarValue(sp.getPredicateVar(), evalBindings);
									Value obj = Algebra.getVarValue(sp.getObjectVar(), evalBindings);
									if ((pred == null || pred.equals(stmt.getPredicate())) && (obj == null || obj.equals(stmt.getObject()))) {
										QuadPattern nq = getQuadPattern(sp, evalBindings);
										if (filterStatement(sp, stmt, nq)) {
											BindingSet spBs = convertStatement(sp, stmt, evalBindings);
											List<BindingSet> bsList = resultsPerSp[i];
											if (bsList == null) {
												resultsPerSp[i] = Collections.singletonList(spBs);
											} else if (bsList.size() == 1) {
												List<BindingSet> newBsList = new ArrayList<>(2);
												newBsList.add(bsList.get(0));
												newBsList.add(spBs);
												resultsPerSp[i] = newBsList;
											} else {
												bsList.add(spBs);
											}
										}
									}
								}
							}
							List<BindingSet> results = resultsPerSp[startIndex];
							if (results == null) {
								return new EmptyIteration<>();
							}
							for (int i=startIndex+1; i<resultsPerSp.length; i++) {
								List<BindingSet> bsList = resultsPerSp[i];
								results = join(results, bsList);
								if (results == null) {
									return new EmptyIteration<>();
								}
							}
							return new CloseableIteratorIteration<>(results.iterator());
		        		}, starJoin, stepBindings, parentStrategy);
		        	} else {
		        		// TODO: getStatements(Resource subj, Set<IRI> preds, Resource... ctxs)
		        		throw new AssertionError();
		        	}
	
		    		if (!isPrebound) {
		    			step = precompileNestedLoopsJoin(firstStep, step, null);	
		    		}
		    		step.evaluate(parent, bindings);
		    	};
	    	}
    	}

    	int i = starJoin.getArgCount() - 1;
    	BindingSetPipeEvaluationStep step = precompileTupleExpr(starJoin.getArg(i), evalContext);
    	for (i--; i>=0; i--) {
    		step = precompileNestedLoopsJoin(precompileTupleExpr(starJoin.getArg(i), evalContext), step, (i==0) ? starJoin : null);
    	}
        return step;
    }

    private static List<BindingSet> join(List<BindingSet> left, List<BindingSet> right) {
    	if (left == null || right == null) {
    		return null;
    	} else {
	    	List<BindingSet> result = new ArrayList<>(left.size()*right.size());
	    	for (BindingSet l : left) {
	    		for (BindingSet r : right) {
	    			BindingSet bs = tryJoin(l, r);
	    			if (bs != null) {
	    				result.add(bs);
	    			}
	    		}
	    	}
	    	return result;
    	}
    }

    private static BindingSet tryJoin(BindingSet left, BindingSet right) {
    	QueryBindingSet result = null;
    	for (Binding b : right) {
    		String name = b.getName();
			Value rv = b.getValue();
    		if (left.hasBinding(name)) {
    			Value lv = left.getValue(name);
    			if (!Objects.equals(lv, rv)) {
    				return null;
    			} else if (result == null) {
    				result = new QueryBindingSet(left);
    			}
    		} else {
    			if (rv != null) {
    				if (result == null) {
    					result = new QueryBindingSet(left);
    				}
    				result.setBinding(name, rv);
    			}
    		}
    	}
    	return result;
    }

    /**
     * Precompile {@link SingletonSet} query model nodes
     * @param singletonSet
     */
    private BindingSetPipeEvaluationStep precompileSingletonSet(SingletonSet singletonSet) {
    	return (parent, bindings) -> {
    		parent = parentStrategy.track(parent, singletonSet);
            parent.pushLast(bindings);
    	};
    }

	/**
	 * Precompile {@link EmptySet} query model nodes
	 * @param emptySet
	 */
	private BindingSetPipeEvaluationStep precompileEmptySet(EmptySet emptySet) {
    	return (parent, bindings) -> {
    		parent = parentStrategy.track(parent, emptySet);
			parent.close(); // nothing to push
    	};
	}

	/**
	 * Precompiles {@link ZeroLengthPath} query model nodes
	 * @param zlp
	 */
	private BindingSetPipeEvaluationStep precompileZeroLengthPath(ZeroLengthPath zlp) {
		return (parent, bindings) -> {
			final Var subjVar = zlp.getSubjectVar();
			final Var objVar = zlp.getObjectVar();
			final Var contextVar = zlp.getContextVar();
			Value subj = Algebra.getVarValue(subjVar, bindings);
			Value obj = Algebra.getVarValue(objVar, bindings);
			if (subj != null && obj != null) {
				if (!subj.equals(obj)) {
					parent.close(); // nothing to push
					return;
				}
			}
	
			if (subj == null && obj == null) {
				Var allSubjVar = Algebra.createAnonVar(ANON_SUBJECT_VAR);
				Var allPredVar = Algebra.createAnonVar(ANON_PREDICATE_VAR);
				Var allObjVar = Algebra.createAnonVar(ANON_OBJECT_VAR);
				StatementPattern sp;
				if (contextVar != null) {
					sp = new StatementPattern(StatementPattern.Scope.NAMED_CONTEXTS, allSubjVar, allPredVar, allObjVar, contextVar.clone());
				} else {
					sp = new StatementPattern(allSubjVar, allPredVar, allObjVar);
				}
				evaluateStatementPattern(new BindingSetPipe(parent) {
					private final BigHashSet<Value> set = BigHashSet.create(collectionMemoryThreshold);
					@Override
					protected boolean next(BindingSet bs) {
						Value ctx = (contextVar != null) ? bs.getValue(contextVar.getName()) : null;
						Value v = bs.getValue(ANON_SUBJECT_VAR);
						if (!nextValue(v, ctx)) {
							return false;
						}
						v = bs.getValue(ANON_OBJECT_VAR);
						if (!nextValue(v, ctx)) {
							return false;
						}
						return true;
					}
					private boolean nextValue(Value v, Value ctx) {
						try {
							if (set.add(v)) {
								QueryBindingSet result = new QueryBindingSet(bindings);
								result.addBinding(subjVar.getName(), v);
								result.addBinding(objVar.getName(), v);
								if (ctx != null) {
									result.addBinding(contextVar.getName(), ctx);
								}
								return parent.push(result);
							} else {
								return true;
							}
						} catch (IOException ioe) {
							return handleException(ioe);
						}
					}
				}, sp, bindings);
			} else {
				QueryBindingSet result = new QueryBindingSet(bindings);
				if (obj == null && subj != null) {
					result.addBinding(objVar.getName(), subj);
				} else if (subj == null && obj != null) {
					result.addBinding(subjVar.getName(), obj);
				} else if (subj != null && subj.equals(obj)) {
					// empty bindings
					// (result but nothing to bind as subjectVar and objVar are both fixed)
				} else {
					result = null;
				}
				if (result != null) {
					parent.push(result);
				}
				parent.close();
			}
		};
	}

	/**
     * Precompiles {@link ArbitraryLengthPath} query model nodes
     * @param alp
     */
    private BindingSetPipeEvaluationStep precompileArbitraryLengthPath(ArbitraryLengthPath alp) {
    	return (parent, bindings) -> {
	        final StatementPattern.Scope scope = alp.getScope();
	        final Var subjectVar = alp.getSubjectVar();
	        final TupleExpr pathExpression = alp.getPathExpression();
	        final Var objVar = alp.getObjectVar();
	        final Var contextVar = alp.getContextVar();
	        final long minLength = alp.getMinLength();
	        //temporary solution using copy of the original iterator
	        //re-writing this to push model is a bit more complex task
            EvaluationStrategy alpStrategy = new DefaultEvaluationStrategy(tripleSource, dataset, null);
//            // Currently causes too many blocked threads
//            EvaluationStrategy alpStrategy = new StrictEvaluationStrategy(null, null) {
//                @Override
//                public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(ZeroLengthPath zlp, BindingSet bindings) throws QueryEvaluationException {
//                    zlp.setParentNode(alp);
//                    return parentStrategy.evaluate(zlp, bindings);
//                }
//
//                @Override
//                public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
//                    expr.setParentNode(alp);
//                    return parentStrategy.evaluate(expr, bindings);
//                }
//            };
            alpStrategy.setTrackResultSize(parentStrategy.isTrackResultSize());
            alpStrategy.setTrackTime(parentStrategy.isTrackTime());
	        try {
	        	executor.pullAndPushAsync(parent, bs -> new PathIteration(alpStrategy, scope, subjectVar, pathExpression, objVar, contextVar, minLength, bs), alp, bindings, parentStrategy);
	        } catch (QueryEvaluationException e) {
	            parent.handleException(e);
	        }
    	};
    }

    /**
     * Precompiles {@link BindingSetAssignment} query model nodes
     * @param bsa
     */
    private BindingSetPipeEvaluationStep precompileBindingSetAssignment(BindingSetAssignment bsa) {
    	return (parent, bindings) -> {
	        if (bindings.isEmpty()) {
	    		try {
		        	for (BindingSet b : bsa.getBindingSets()) {
		        		if (!parent.push(b)) {
		        			return;
		        		}
		        	}
	            } finally {
	            	parent.close();
	            }
	        } else {
	    		try {
		        	for (BindingSet assignedBindings : bsa.getBindingSets()) {
	                    QueryBindingSet result;
	                    if (assignedBindings.isEmpty()) {
	                    	result = new QueryBindingSet(bindings);
	                    } else {
	                    	result = null;
		                    for (String name : assignedBindings.getBindingNames()) {
		                        final Value assignedValue = assignedBindings.getValue(name);
		                        if (assignedValue != null) { // can be null if set to UNDEF
		                            // check that the binding assignment does not overwrite
		                            // existing bindings.
		                            Value bValue = bindings.getValue(name);
		                            if (bValue == null || assignedValue.equals(bValue)) {
		                            	// values are compatible - create a result if it doesn't already exist
	                                    if (result == null) {
	                                        result = new QueryBindingSet(bindings);
	                                    }
		                                if (bValue == null) {
		                                    // we are not overwriting an existing binding.
		                                    result.addBinding(name, assignedValue);
		                                }
		                            } else {
		                                // if values are not equal there is no compatible
		                                // merge and we should return no next element.
		                                result = null;
		                                break;
		                            }
		                        }
		                    }
	                    }
	
	                    if (result != null) {
			        		if (!parent.push(result)) {
			        			return;
			        		}
		        		}
		        	}
	            } finally {
	            	parent.close();
	            }
	        }
    	};
    }

    /**
	 * Precompile {@link TupleFunctionCall} query model nodes
	 * 
	 * @param tfc
	 */
	private BindingSetPipeEvaluationStep precompileTupleFunctionCall(ExtendedTupleFunctionCall tfc, QueryEvaluationContext evalContext)
			throws QueryEvaluationException {
		TupleFunction func = parentStrategy.tupleFunctionRegistry.get(tfc.getURI())
				.orElseThrow(() -> new QueryEvaluationException("Unknown tuple function '" + tfc.getURI() + "'"));

		List<ValueExpr> args = tfc.getArgs();
		ValuePipeQueryValueEvaluationStep[] argSteps = new ValuePipeQueryValueEvaluationStep[args.size()];
		for (int i = 0; i < args.size(); i++) {
			argSteps[i] = parentStrategy.precompile(args.get(i), evalContext);
		}

		java.util.function.Function<Value[],CloseableIteration<? extends List<? extends Value>, QueryEvaluationException>> tfEvaluator = TupleFunctionEvaluationStrategy.createEvaluator(func, tripleSource);
		java.util.function.Function<BindingSetPipe,BindingSetPipe> pipeBuilder = parent -> {
			return new BindingSetPipe(parent) {
				@Override
				protected boolean next(BindingSet bs) {
					try {
						Value[] argValues = new Value[argSteps.length];
						for (int i = 0; i < argSteps.length; i++) {
							argValues[i] = argSteps[i].evaluate(bs);
						}
	
						try (CloseableIteration<BindingSet, QueryEvaluationException> iter = TupleFunctionEvaluationStrategy.createBindings(tfEvaluator.apply(argValues), tfc.getResultVars(), bs)) {
							while (iter.hasNext()) {
								if(!parent.push(iter.next())) {
									return false;
								}
							}
						}
					} catch (ValueExprEvaluationException ignore) {
						// can't evaluate arguments
						LOGGER.trace("Failed to evaluate " + tfc.getURI(), ignore);
					}
					return true;
				}
				@Override
				public String toString() {
					return "TupleFunctionCallBindingSetPipe";
				}
			};
		};

		TupleExpr depExpr = tfc.getDependentExpression();
		if (depExpr != null) {
			BindingSetPipeEvaluationStep step = precompileTupleExpr(depExpr, evalContext);
			return (parent, bindings) -> {
				step.evaluate(pipeBuilder.apply(parent), bindings);
			};
		} else {
			// dependencies haven't been identified, but we'll try to evaluate anyway
			return (parent, bindings) -> {
				BindingSetPipe pipe = pipeBuilder.apply(parent);
				pipe.pushLast(bindings);
			};
		}
	}

	/**
	 * Returns the limit of the current variable bindings before any further
	 * projection.
	 */
    private static long getLimit(QueryModelNode node) {
        long offset = 0;
        if (node instanceof Slice) {
            Slice slice = (Slice) node;
            if (slice.hasOffset() && slice.hasLimit()) {
                return slice.getOffset() + slice.getLimit();
            } else if (slice.hasLimit()) {
                return slice.getLimit();
            } else if (slice.hasOffset()) {
                offset = slice.getOffset();
            }
        }
        QueryModelNode parent = node.getParentNode();
        if (parent instanceof Distinct || parent instanceof Reduced || parent instanceof Slice) {
            long limit = getLimit(parent);
            if (offset > 0L && limit < Long.MAX_VALUE) {
                return offset + limit;
            } else {
                return limit;
            }
        }
        return Long.MAX_VALUE;
    }

    /**
     * Determines if the parent of the node is an instance of {@link Distinct} or {@link Reduced}.
     * @param node the {@link QueryModelNode} to test
     * @return {@code true} if the parent is and instance of {@link Distinct} or {@link Reduced} and {@code false} otherwise. If the parent is
     * an instance of {@link Slice} then the parent is considered to be the first non-{@code Slice} node up the tree.
     */
    private static boolean isReducedOrDistinct(QueryModelNode node) {
        QueryModelNode parent = node.getParentNode();
        if (parent instanceof Slice) {
            return isReducedOrDistinct(parent);
        }
        return parent instanceof Distinct || parent instanceof Reduced;
    }


    static abstract class PipeJoin extends BindingSetPipe {
    	private final AtomicLong inProgress = new AtomicLong();
    	private final AtomicBoolean finished = new AtomicBoolean();

    	PipeJoin(BindingSetPipe parent) {
    		super(parent);
    	}
    	protected BindingSetPipe getParent() {
    		return parent;
    	}
    	protected final void startSecondaryPipe() {
    		inProgress.incrementAndGet();
    	}
    	/**
    	 * @param isLast if known else false
    	 */
    	protected final void startSecondaryPipe(boolean isLast) {
    		inProgress.incrementAndGet();
    		if (isLast) {
    			finished.set(true);
    		}
    	}
    	protected final boolean pushToParent(BindingSet bs) {
    		boolean pushMore = parent.push(bs);
    		if (!pushMore) {
    			finished.set(true);
    		}
    		return pushMore;
    	}
    	protected final void endSecondaryPipe() {
    		inProgress.decrementAndGet();
    		// close if we are the last child and the main pipe has already finished
    		if (finished.get() && inProgress.compareAndSet(0L, -1L)) {
    			parent.close();
    		}
    	}
    	@Override
    	protected void doClose() {
    		finished.set(true);
    		// close if all children have already finished
    		if (inProgress.compareAndSet(0L, -1L)) {
   				parent.close();
    		}
    	}
    }


	static abstract class BindingSetValuePipe extends ValuePipe {
		protected final BindingSetPipe parent;
		protected BindingSetValuePipe(BindingSetPipe parent) {
			super(null);
			this.parent = parent;
		}
		@Override
		public final void handleException(Throwable e) {
			parent.handleException(e);
		}
	}
}
