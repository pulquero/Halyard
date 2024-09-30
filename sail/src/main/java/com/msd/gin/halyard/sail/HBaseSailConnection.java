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
package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.optimizers.HalyardConstantOptimizer;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.TupleFunctionCallOptimizer;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.BindingSetPipeQueryEvaluationStep;
import com.msd.gin.halyard.query.CloseableConsumer;
import com.msd.gin.halyard.query.TimeLimitConsumer;
import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.ServiceRoot;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.impl.ExtendedEvaluationStrategy;
import com.msd.gin.halyard.sail.HBaseSail.SailConnectionFactory;
import com.msd.gin.halyard.spin.SpinFunctionInterpreter;
import com.msd.gin.halyard.spin.SpinMagicPropertyInterpreter;
import com.msd.gin.halyard.strategy.ExtendedQueryOptimizerPipeline;
import com.msd.gin.halyard.strategy.HalyardEvaluationExecutor;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;
import com.msd.gin.halyard.util.MBeanManager;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.common.iteration.IterationWrapper;
import org.eclipse.rdf4j.common.iteration.ReducedIteration;
import org.eclipse.rdf4j.common.iteration.TimeLimitIteration;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ParentReferenceCleaner;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.StandardQueryOptimizerPipeline;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSailConnection extends AbstractSailConnection implements BindingSetConsumerSailConnection, BindingSetPipeSailConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSailConnection.class);

	private static final String INTERNAL_BINDING_PREFIX = "__halyard_";
	private static final String INTERNAL_BINDING_SUFFIX = "__";

	public static final String SOURCE_STRING_BINDING = internalBinding("source");
	public static final String UPDATE_PART_BINDING = internalBinding("update_part");
	private static final int NO_UPDATE_PARTS = -1;
	public static final String FORK_INDEX_BINDING = internalBinding("fork_index");
	private static final String CONNECTION_ID_ATTRIBUTE = "connectionId";

	public static enum DefaultGraphMode {
		IMPLICIT, EXPLICIT
	};

	private final HBaseSail sail;
	private final KeyValueMapper insertDefaultKeyValueMapper;
	private final KeyValueMapper insertNonDefaultKeyValueMapper;
	private final KeyValueMapper deleteDefaultKeyValueMapper;
	private final KeyValueMapper deleteNonDefaultKeyValueMapper;
	private final String id;
	private final boolean usePush;
	private boolean trackBranchOperatorsOnly;
	private KeyspaceConnection keyspaceConn;
	private HalyardEvaluationExecutor executor;
	private boolean executorIsShared;
	private BufferedMutator mutator;
	private int pendingUpdateCount;
	private boolean flushWritesBeforeReads = true;
	private KeyValueMapper insertCurrentKeyValueMapper;
	private KeyValueMapper deleteCurrentKeyValueMapper;
	private long lastTimestamp = Long.MIN_VALUE;
	private boolean lastUpdateWasDelete;
	private long beginTimestamp = Timestamped.NOT_SET;

	public HBaseSailConnection(HBaseSail sail) throws IOException {
		this(sail, null);
	}

	HBaseSailConnection(HBaseSail sail, HalyardEvaluationExecutor executor) throws IOException {
		this.sail = sail;
		this.insertDefaultKeyValueMapper = sail.getStatementIndices()::insertKeyValues;
		this.insertNonDefaultKeyValueMapper = sail.getStatementIndices()::insertNonDefaultKeyValues;
		this.deleteDefaultKeyValueMapper = sail.getStatementIndices()::deleteKeyValues;
		this.deleteNonDefaultKeyValueMapper = sail.getStatementIndices()::deleteNonDefaultKeyValues;
		this.insertCurrentKeyValueMapper = this.insertDefaultKeyValueMapper;
		this.deleteCurrentKeyValueMapper = this.deleteDefaultKeyValueMapper;
		this.id = MBeanManager.getId(this);
		this.usePush = sail.pushStrategy;
		this.executor = executor;
		this.executorIsShared = (executor != null);
		// tables are lightweight but not thread-safe so get a new instance per sail
		// connection
		this.keyspaceConn = sail.keyspace.getConnection();
		sail.connectionOpened(this);
    }

	String getId() {
		return id;
	}

	public boolean isTrackBranchOperatorsOnly() {
		return trackBranchOperatorsOnly;
	}

	public void setTrackBranchOperatorsOnly(boolean f) {
		trackBranchOperatorsOnly = f;
	}

	public boolean isFlushWritesBeforeReadsEnabled() {
		return flushWritesBeforeReads;
	}

	public void setFlushWritesBeforeReadsEnabled(boolean f) {
		flushWritesBeforeReads = f;
	}

	public DefaultGraphMode getDefaultGraphInsertMode() {
		if (insertCurrentKeyValueMapper == insertNonDefaultKeyValueMapper) {
			return DefaultGraphMode.EXPLICIT;
		} else {
			return DefaultGraphMode.IMPLICIT;
		}
	}

	public void setDefaultGraphInsertMode(DefaultGraphMode mode) {
		if (mode == DefaultGraphMode.EXPLICIT) {
			insertCurrentKeyValueMapper = insertNonDefaultKeyValueMapper;
		} else {
			insertCurrentKeyValueMapper = insertDefaultKeyValueMapper;
		}
	}

	public DefaultGraphMode getDefaultGraphDeleteMode() {
		if (deleteCurrentKeyValueMapper == deleteNonDefaultKeyValueMapper) {
			return DefaultGraphMode.EXPLICIT;
		} else {
			return DefaultGraphMode.IMPLICIT;
		}
	}

	public void setDefaultGraphDeleteMode(DefaultGraphMode mode) {
		if (mode == DefaultGraphMode.EXPLICIT) {
			deleteCurrentKeyValueMapper = deleteNonDefaultKeyValueMapper;
		} else {
			deleteCurrentKeyValueMapper = deleteDefaultKeyValueMapper;
		}
	}

	private HalyardEvaluationExecutor getExecutor() {
		if (!isOpen()) {
			throw new SailException("Connection is closed");
		}
		if (executor == null) {
			Map<String, String> attrs = new LinkedHashMap<>();
			attrs.put(CONNECTION_ID_ATTRIBUTE, getId());
			attrs.putAll(sail.getConnectionAttributes(MBeanManager.getId(sail)));
			String sourceName = (sail.tableName != null) ? sail.tableName.getNameAsString() : sail.snapshotName;
			// snapshots: due to region file locking must open/close iterator from the same thread!
			boolean useAsyncPullPush = (sail.snapshotName == null);
			executor = HalyardEvaluationExecutor.create(sourceName, sail.getConfiguration(), useAsyncPullPush, attrs);
		}
		return executor;
	}

	private BufferedMutator getBufferedMutator() {
		if (!isOpen()) {
			throw new SailException("Connection is closed");
		}
		if (mutator == null) {
			mutator = sail.getBufferedMutator();
    	}
    	return mutator;
    }

    @Override
    public boolean isOpen() throws SailException {
		return keyspaceConn != null;
    }

    @Override
    public void close() throws SailException {
		beginTimestamp = Timestamped.NOT_SET;
		try {
			flush();
			if (!executorIsShared && executor != null) {
				executor.shutdown();
				executor = null;
			}
			if (mutator != null) {
				try {
					mutator.close();
				} catch (IOException e) {
					throw new SailException(e);
				} finally {
					mutator = null;
				}
			}

			if (keyspaceConn != null) {
				try {
					keyspaceConn.close();
				} catch (IOException e) {
					throw new SailException(e);
				} finally {
					keyspaceConn = null;
				}
			}
		} finally {
			sail.connectionClosed(this);
		}
    }

	private EvaluationStrategy createEvaluationStrategy(TripleSource source, Dataset dataset, boolean isPartitioned) {
		EvaluationStrategy strategy;
		HalyardEvaluationStatistics stats = sail.getStatistics();
		if (usePush) {
			strategy = new HalyardEvaluationStrategy(sail.getStrategyConfig(), source, sail.getTupleFunctionRegistry(), sail.getFunctionRegistry(), sail.getAggregateFunctionRegistry(), dataset, sail.getFederatedServiceResolver(), stats,
					getExecutor(), isPartitioned);
		} else {
			strategy = new ExtendedEvaluationStrategy(source, dataset, sail.getFederatedServiceResolver(), sail.getTupleFunctionRegistry(), sail.getFunctionRegistry(), 0L, stats);
			strategy.setOptimizerPipeline(new ExtendedQueryOptimizerPipeline(strategy, source.getValueFactory(), stats));
		}
		if (trackResultSize) {
			strategy.setTrackResultSize(trackResultSize);
		}
		if (trackResultTime) {
			strategy.setTrackTime(trackResultTime);
		}
		if (trackBranchOperatorsOnly && (strategy instanceof HalyardEvaluationStrategy)) {
			((HalyardEvaluationStrategy) strategy).setTrackBranchOperatorsOnly(trackBranchOperatorsOnly);
		}
		return strategy;
	}

	private TupleExpr optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, final boolean includeInferred, HBaseTripleSource tripleSource, QueryOptimizer optimizer) {
		if (cloneTupleExpression) {
			tupleExpr = tupleExpr.clone();
		}

		// Add a dummy root node to the tuple expressions to allow the
		// optimizers to modify the actual root node
		tupleExpr = Algebra.ensureRooted(tupleExpr);

		new SpinFunctionInterpreter(sail.getSpinParser(), tripleSource, sail.getFunctionRegistry()).optimize(tupleExpr, dataset, bindings);
		if (includeInferred) {
			new SpinMagicPropertyInterpreter(sail.getSpinParser(), tripleSource, sail.getTupleFunctionRegistry(), sail.getFederatedServiceResolver()).optimize(tupleExpr, dataset, bindings);
		}
		tripleSource.optimize(tupleExpr, dataset, bindings);
		LOGGER.trace("Query tree after interpretation:\n{}", tupleExpr);

		optimizer.optimize(tupleExpr, dataset, bindings);

		// required for correct evaluation of tuple functions
		new TupleFunctionCallOptimizer().optimize(tupleExpr, dataset, bindings);
		new ParentReferenceCleaner().optimize(tupleExpr, dataset, bindings);
		return tupleExpr;
	}

	TupleExpr optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred, HBaseTripleSource tripleSource, EvaluationStrategy strategy) {
		return optimize(tupleExpr, dataset, bindings, includeInferred, tripleSource, (te, d, b) -> strategy.optimize(te, sail.getStatistics(), b));
	}

	private TupleExpr bindOptimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred, HBaseTripleSource tripleSource, EvaluationStrategy strategy) {
		return optimize(tupleExpr, dataset, bindings, includeInferred, tripleSource, (te, d, b) -> {
			StandardQueryOptimizerPipeline.BINDING_ASSIGNER.optimize(te, d, b);
			new HalyardConstantOptimizer(strategy, tripleSource.getValueFactory()).optimize(te, d, b);
		});
	}

	private TupleExpr getOptimizedQuery(String sourceString, int updatePart, TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred, boolean isPartitioned, HBaseTripleSource tripleSource,
			EvaluationStrategy strategy) {
		LOGGER.debug("Query tree before optimization:\n{}", tupleExpr);
		TupleExpr optimizedTree;
		if (tupleExpr instanceof ServiceRoot) {
			optimizedTree = bindOptimize(tupleExpr, dataset, bindings, includeInferred, tripleSource, strategy);
			LOGGER.debug("Query tree after optimization (binding-optimized):\n{}", optimizedTree);
		} else if (sourceString != null && cloneTupleExpression) {
			optimizedTree = sail.queryCache.getOptimizedQuery(this, sourceString, updatePart, tupleExpr, dataset, bindings, includeInferred, isPartitioned, tripleSource, strategy);
			LOGGER.debug("Query tree after optimization (cached):\n{}", optimizedTree);
		} else {
			optimizedTree = optimize(tupleExpr, dataset, bindings, includeInferred, tripleSource, strategy);
			LOGGER.debug("Query tree after optimization:\n{}", optimizedTree);
		}
		return optimizedTree;
	}

	// evaluate queries/ subqueries
    @Override
	public CloseableIteration<BindingSet> evaluate(final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) throws SailException {
		return evaluate((optimizedTree, step, queryInfo) -> {
			try {
				CloseableIteration<BindingSet> iter = evaluateInternal(optimizedTree, step);
				iter = new IterationWrapper<BindingSet>(iter) {
					@Override
					protected void handleClose() throws QueryEvaluationException {
						try {
							super.handleClose();
						} finally {
							queryInfo.end();
						}
					}
				};
				return sail.evaluationTimeoutSecs <= 0 ? iter
						: new TimeLimitIteration<BindingSet>(iter, TimeUnit.SECONDS.toMillis(sail.evaluationTimeoutSecs)) {
							@Override
							protected void throwInterruptedException() {
								throw new QueryInterruptedException(String.format("Query evaluation exceeded specified timeout %ds", sail.evaluationTimeoutSecs));
							}
						};
			} catch (QueryEvaluationException ex) {
				throw new SailException(ex);
			}
		}, tupleExpr, dataset, bindings, includeInferred);
    }

	@Override
	public void evaluate(Consumer<BindingSet> handler, final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) {
		evaluate((optimizedTree, step, queryInfo) -> {
			try {
				try (CloseableConsumer<BindingSet> timeLimitHandler = TimeLimitConsumer.apply(handler, sail.evaluationTimeoutSecs)) {
					evaluateInternal(timeLimitHandler, optimizedTree, step);
				}
			} catch (QueryEvaluationException ex) {
				throw new SailException(ex);
			} finally {
				queryInfo.end();
			}
			return null;
		}, tupleExpr, dataset, bindings, includeInferred);
	}

	@Override
	public void evaluate(BindingSetPipe pipe, final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) {
		evaluate((optimizedTree, step, queryInfo) -> {
			BindingSetPipe queryEndPipe = new BindingSetPipe(pipe) {
				@Override
				protected void doClose() {
					try {
						super.doClose();
					} finally {
						queryInfo.end();
					}
				}

				@Override
				public boolean handleException(Throwable e) {
					try {
						return super.handleException(e);
					} finally {
						queryInfo.end();
					}
				}
			};
			try {
				evaluateInternal(queryEndPipe, optimizedTree, step);
			} catch (QueryEvaluationException ex) {
				throw new SailException(ex);
			}
			return null;
		}, tupleExpr, dataset, bindings, includeInferred);
	}

	private <E> E evaluate(QueryEvaluator<E> evaluator, final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred) {
		if (flushWritesBeforeReads) {
			flush();
		}

		String sourceString = Literals.getLabel(bindings.getValue(SOURCE_STRING_BINDING), null);
		int updatePart = Literals.getIntValue(bindings.getValue(UPDATE_PART_BINDING), NO_UPDATE_PARTS);
		int forkIndex = Literals.getIntValue(bindings.getValue(FORK_INDEX_BINDING), StatementIndices.NO_PARTITIONING);
		BindingSet queryBindings = removeImplicitBindings(bindings);

		boolean isPartitioned = (forkIndex >= 0);
		if (isPartitioned) {
			LOGGER.debug("Partition index is {}", forkIndex);
		}

		HBaseTripleSource tripleSource = sail.createTripleSource(keyspaceConn, includeInferred, forkIndex);
		EvaluationStrategy strategy = createEvaluationStrategy(tripleSource, dataset, isPartitioned);

		TupleExpr optimizedTree = getOptimizedQuery(sourceString, updatePart, tupleExpr, dataset, queryBindings, includeInferred, isPartitioned, tripleSource, strategy);
		QueryEvaluationContext evalContext = new QueryEvaluationContext.Minimal(dataset, tripleSource.getValueFactory());
		QueryEvaluationStep step = strategy.precompile(optimizedTree, evalContext);
		HBaseSail.QueryInfo queryInfo = sail.trackQuery(this, sourceString, tupleExpr, optimizedTree);
		return evaluator.evaluate(optimizedTree, step, queryInfo);
	}

	private BindingSet removeImplicitBindings(BindingSet bs) {
		QueryBindingSet cleaned = new QueryBindingSet();
		for (Binding b : bs) {
			if (!isInternalBinding(b.getName())) {
				cleaned.addBinding(b);
			}
		}
		// canonicalise
		return (!cleaned.isEmpty()) ? cleaned : EmptyBindingSet.getInstance();
	}

	protected CloseableIteration<BindingSet> evaluateInternal(TupleExpr optimizedTree, QueryEvaluationStep step) throws QueryEvaluationException {
		return step.evaluate(EmptyBindingSet.getInstance());
	}

	protected void evaluateInternal(Consumer<BindingSet> handler, TupleExpr optimizedTree, QueryEvaluationStep step) throws QueryEvaluationException {
		if (step instanceof BindingSetPipeQueryEvaluationStep) {
			((BindingSetPipeQueryEvaluationStep) step).evaluate(handler, EmptyBindingSet.getInstance());
		} else {
			BindingSetConsumerSailConnection.report(step.evaluate(EmptyBindingSet.getInstance()), handler);
		}
	}

	/**
	 * NB: asynchronous.
	 * 
	 * @param pipe
	 * @param optimizedTree
	 * @param step
	 * @throws QueryEvaluationException
	 */
	protected void evaluateInternal(BindingSetPipe pipe, TupleExpr optimizedTree, QueryEvaluationStep step) throws QueryEvaluationException {
		if (step instanceof BindingSetPipeQueryEvaluationStep) {
			((BindingSetPipeQueryEvaluationStep) step).evaluate(pipe, EmptyBindingSet.getInstance());
		} else {
			BindingSetPipeSailConnection.report(step.evaluate(EmptyBindingSet.getInstance()), pipe);
			pipe.close();
		}
	}

    @Override
	public CloseableIteration<? extends Resource> getContextIDs() throws SailException {

        //generate an iterator over the identifiers of the contexts available in Halyard.
		final CloseableIteration<? extends Statement> scanner = getStatements(HALYARD.STATS_ROOT_NODE, SD.NAMED_GRAPH_PROPERTY, null, true, HALYARD.STATS_GRAPH_CONTEXT);
		if (scanner.hasNext()) {
			return new ConvertingIteration<Statement, Resource>(scanner) {

				@Override
				protected Resource convert(Statement stmt) {
					return (IRI) stmt.getObject();
				}

			};
		} else {
			scanner.close();

			if (sail.evaluationTimeoutSecs > 0) {
				// try to find them manually if there are no stats and there is a specific timeout
				class StatementScanner extends AbstractStatementScanner {
					final ResultScanner rs;

					StatementScanner(RDFFactory rdfFactory) throws IOException {
						super(sail.getStatementIndices(), sail.getValueFactory());
						rs = keyspaceConn.getScanner(sail.getStatementIndices().getCSPOIndex().scan());
					}

					@Override
					protected Result nextResult() {
						try {
							return rs.next();
						} catch (IOException ioe) {
							throw new QueryEvaluationException(ioe);
						}
					}

					@Override
					protected void handleClose() {
						rs.close();
					}
				}

				try {
					return new TimeLimitIteration<Resource>(
							new ReducedIteration<Resource>(new ConvertingIteration<Statement, Resource>(new ExceptionConvertingIteration<Statement, SailException>(new StatementScanner(sail.getRDFFactory())) {
								@Override
								protected SailException convert(RuntimeException e) {
									return new SailException(e);
								}
							}) {
								@Override
								protected Resource convert(Statement stmt) {
									return stmt.getContext();
								}
							}), TimeUnit.SECONDS.toMillis(sail.evaluationTimeoutSecs)) {
						@Override
						protected void throwInterruptedException() {
							throw new SailException(String.format("Evaluation exceeded specified timeout %ds", sail.evaluationTimeoutSecs));
						}
					};
				} catch (IOException ioe) {
					throw new SailException(ioe);
				}
			} else {
				return new EmptyIteration<>();
			}
		}
    }

    @Override
	public CloseableIteration<? extends Statement> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
		if (flushWritesBeforeReads) {
			flush();
		}
		TripleSource tripleSource = sail.createTripleSource(keyspaceConn, includeInferred);
		return new ExceptionConvertingIteration<Statement, SailException>(tripleSource.getStatements(subj, pred, obj, contexts)) {
			@Override
			protected SailException convert(RuntimeException e) {
				throw new SailException(e);
			}
		};
    }

    @Override
	public boolean hasStatement(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
		for (Resource ctx : contexts) {
			if (ctx != null && ctx.isTriple()) {
				return false;
			}
		}
		if (flushWritesBeforeReads) {
			flush();
		}
		ExtendedTripleSource tripleSource = sail.createTripleSource(keyspaceConn, includeInferred);
		return tripleSource.hasStatement(subj, pred, obj, contexts);
	}

	@Override
    public synchronized long size(Resource... contexts) throws SailException {
        long size = 0;
        if (contexts != null && contexts.length > 0 && contexts[0] != null) {
            for (Resource ctx : contexts) {
            		//examine the VOID statistics for the count of triples in this context
					try (CloseableIteration<? extends Statement> scanner = getStatements(ctx, VOID.TRIPLES, null, true, HALYARD.STATS_GRAPH_CONTEXT)) {
                    if (scanner.hasNext()) {
                        size += ((Literal)scanner.next().getObject()).longValue();
                    }
                    if (scanner.hasNext()) {
                        throw new SailException("Multiple different values exist in VOID statistics for context: "+ctx.stringValue()+". Considering removing and recomputing statistics");
                    }
                }
            }
        } else {
			try (CloseableIteration<? extends Statement> scanner = getStatements(HALYARD.STATS_ROOT_NODE, VOID.TRIPLES, null, true, HALYARD.STATS_GRAPH_CONTEXT)) {
                if (scanner.hasNext()) {
                    size += ((Literal)scanner.next().getObject()).longValue();
                }
                if (scanner.hasNext()) {
                    throw new SailException("Multiple different values exist in VOID statistics. Considering removing and recomputing statistics");
                }
            }
        }
        // try to count it manually if there are no stats and there is a specific timeout
		if (size == 0 && sail.evaluationTimeoutSecs > 0) {
			try (CloseableIteration<? extends Statement> scanner = getStatements(null, null, null, true, contexts)) {
				while (scanner.hasNext()) {
					scanner.next();
					size++;
				}
			}
		}
        return size;
    }

    @Override
	public void begin() throws SailException {
		begin(null);
    }

    @Override
    public void begin(IsolationLevel level) throws UnknownSailTransactionStateException, SailException {
        if (level != null && level != IsolationLevels.NONE) {
            throw new UnknownSailTransactionStateException("Isolation level " + level + " is not compatible with this HBaseSail");
        }
		beginTimestamp = System.currentTimeMillis();
    }

    @Override
    public void flush() throws SailException {
		if (pendingUpdateCount > 0) {
			try {
				mutator.flush();
				pendingUpdateCount = 0;
			} catch (IOException e) {
				throw new SailException(e);
			}
		}
    }

    @Override
    public void prepare() throws SailException {
    }

    @Override
    public void commit() throws SailException {
		beginTimestamp = Timestamped.NOT_SET;
    }

    @Override
    public void rollback() throws SailException {
		beginTimestamp = Timestamped.NOT_SET;
    }

    @Override
    public boolean isActive() throws UnknownSailTransactionStateException {
		return beginTimestamp != Timestamped.NOT_SET;
    }

	private long getTimestamp(long updateTimestamp, boolean isDelete) {
		long ts = updateTimestamp;
		if (ts == Timestamped.NOT_SET) {
			ts = getDefaultTimestamp(isDelete);
		}
		return ts;
	}

	private long getUpdateTimestamp(UpdateContext op) {
		return (op instanceof Timestamped) ? ((Timestamped) op).getTimestamp() : Timestamped.NOT_SET;
	}

	/**
	 * Timestamp to use if none specified by the UpdateContext, e.g. via halyard:timestamp.
	 * 
	 * @param isDelete flag to indicate timestamp is for a delete operation
	 * @return millisecond timestamp
	 */
	protected long getDefaultTimestamp(boolean isDelete) {
		long ts = beginTimestamp;
		if (ts == Timestamped.NOT_SET) {
			ts = System.currentTimeMillis();
		}
		if (ts > lastTimestamp) {
			lastTimestamp = ts;
		} else {
			if (!lastUpdateWasDelete && isDelete) {
				lastTimestamp++; // ensure delete is ordered after any previous add
			}
			ts = lastTimestamp;
		}
		lastUpdateWasDelete = isDelete;
		return ts;
    }

    @Override
    public void addStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		addStatement(null, subj, pred, obj, contexts);
    }

    @Override
    public void addStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		checkWritable();
		long timestamp = getTimestamp(getUpdateTimestamp(op), false);
		if (contexts == null || contexts.length == 0) {
			// if all contexts then insert into the default context
			contexts = new Resource[] { null };
		}
        try {
			for (Resource ctx : contexts) {
				insertStatement(subj, pred, obj, ctx, timestamp, insertCurrentKeyValueMapper);
			}
        } catch (IOException e) {
            throw new SailException(e);
        }
    }

	public void addStatement(Resource subj, IRI pred, Value obj, Resource ctx, long updateTimestamp) throws SailException {
		checkWritable();
		long timestamp = getTimestamp(updateTimestamp, false);
		try {
			insertStatement(subj, pred, obj, ctx, timestamp, insertCurrentKeyValueMapper);
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	public void addSystemStatement(Resource subj, IRI pred, Value obj, Resource ctx, long timestamp) throws SailException {
		checkWritable();
		try {
			insertStatement(subj, pred, obj, ctx, timestamp, insertNonDefaultKeyValueMapper);
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

    private void checkWritable() {
		if (!isOpen()) {
			throw new IllegalStateException("Connection is closed");
		}
		if (!sail.isWritable()) {
			throw new SailException(sail.tableName + " is read only");
		}
    }

	protected int insertStatement(Resource subj, IRI pred, Value obj, @Nullable Resource ctx, long timestamp, KeyValueMapper keyValueMapper) throws IOException {
		if (ctx != null && ctx.isTriple()) {
			throw new SailException("context argument can not be of type Triple: " + ctx);
		}
		List<? extends KeyValue> kvs = keyValueMapper.toKeyValues(subj, pred, obj, ctx, timestamp);
		for (KeyValue kv : kvs) {
			put(kv);
		}
		return kvs.size();
	}

	protected void put(KeyValue kv) throws IOException {
		getBufferedMutator().mutate(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
		pendingUpdateCount++;
	}
	
	@Override
	public void removeStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		removeUnknownStatements(subj, pred, obj, contexts, Timestamped.NOT_SET);
	}

	@Override
	public void removeStatement(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		long updateTimestamp = getUpdateTimestamp(op);
		removeStatement(subj, pred, obj, contexts, updateTimestamp);
	}

	public void removeStatement(Resource subj, IRI pred, Value obj, Resource[] contexts, long updateTimestamp) throws SailException {
		if (subj != null && pred != null && obj != null && contexts != null && contexts.length > 0) {
			removeKnownStatement(subj, pred, obj, contexts, updateTimestamp);
		} else {
			removeUnknownStatements(subj, pred, obj, contexts, updateTimestamp);
		}
	}

	private void removeKnownStatement(Resource subj, IRI pred, Value obj, Resource[] contexts, long updateTimestamp) throws SailException {
		checkWritable();
		long timestamp = getTimestamp(updateTimestamp, true);
		try {
			for (Resource ctx : contexts) {
				deleteStatement(subj, pred, obj, ctx, timestamp, deleteCurrentKeyValueMapper);
			}
			if (subj.isTriple()) {
				removeTriple((Triple) subj, timestamp);
			}
			if (obj.isTriple()) {
				removeTriple((Triple) obj, timestamp);
			}
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	private long removeUnknownStatements(@Nullable Resource subjPattern, @Nullable IRI predPattern, @Nullable Value objPattern, @Nullable Resource[] contexts, long updateTimestamp) throws SailException {
		checkWritable();
		if (subjPattern == null && predPattern == null && objPattern == null && (contexts == null || contexts.length == 0)) {
			clearAllStatements();
			return -1L;
		} else {
			final long timestamp = getTimestamp(updateTimestamp, true);
			long counter = 0L;

			final class TripleSet {
				final int bufferSize = 1000;
				Set<Triple> triples;

				void add(Triple t) throws IOException {
					if (triples == null) {
						triples = new HashSet<>(bufferSize + 1);
					}
					triples.add(t);
					if (triples.size() > bufferSize) {
						removeAll();
					}
				}

				void removeAll() throws IOException {
					if (triples != null) {
						for (Triple triple : triples) {
							removeTriple(triple, timestamp);
						}
						triples = null;
					}
				}
			}

			TripleSet triples = new TripleSet();
			try (CloseableIteration<? extends Statement> iter = getStatements(subjPattern, predPattern, objPattern, true, contexts)) {
				while (iter.hasNext()) {
					Statement st = iter.next();
					Resource subj = st.getSubject();
					IRI pred = st.getPredicate();
					Value obj = st.getObject();
					deleteStatement(subj, pred, obj, st.getContext(), timestamp, deleteCurrentKeyValueMapper);
					if (subj.isTriple()) {
						triples.add((Triple) subj);
					}
					if (obj.isTriple()) {
						triples.add((Triple) obj);
					}
					counter++;
				}
				triples.removeAll();
			} catch (IOException e) {
				throw new SailException(e);
			}
			return counter;
		}
	}

	private void removeTriple(Triple t, Long timestamp) throws IOException {
		flush();
		if (!sail.getStatementIndices().isTripleReferenced(keyspaceConn, t)) {
			// orphaned so safe to remove
			deleteStatement(t.getSubject(), t.getPredicate(), t.getObject(), HALYARD.TRIPLE_GRAPH_CONTEXT, timestamp, deleteNonDefaultKeyValueMapper);
			if (t.getSubject().isTriple()) {
				removeTriple((Triple) t.getSubject(), timestamp);
			}
			if (t.getObject().isTriple()) {
				removeTriple((Triple) t.getObject(), timestamp);
			}
		}
	}

	public void removeSystemStatement(Resource subj, IRI pred, Value obj, Resource context, long timestamp) throws SailException {
		checkWritable();
		try {
			deleteStatement(subj, pred, obj, context, timestamp, deleteNonDefaultKeyValueMapper);
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	private void deleteSystemStatements(@Nullable Resource subj, @Nullable IRI pred, @Nullable Value obj, Resource[] contexts, long timestamp) throws SailException {
		try (CloseableIteration<? extends Statement> iter = getStatements(subj, pred, obj, true, contexts)) {
			while (iter.hasNext()) {
				Statement st = iter.next();
				deleteStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext(), timestamp, deleteNonDefaultKeyValueMapper);
			}
		} catch (IOException e) {
			throw new SailException(e);
		}
	}

	protected int deleteStatement(Resource subj, IRI pred, Value obj, Resource ctx, long timestamp, KeyValueMapper keyValueMapper) throws IOException {
		List<? extends KeyValue> kvs = keyValueMapper.toKeyValues(subj, pred, obj, ctx, timestamp);
		for (KeyValue kv : kvs) {
			delete(kv);
		}
		return kvs.size();
	}

	protected void delete(KeyValue kv) throws IOException {
		getBufferedMutator().mutate(new Delete(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()).add(kv));
		pendingUpdateCount++;
	}

    @Override
    public void startUpdate(UpdateContext op) throws SailException {
    }

    @Override
    public void endUpdate(UpdateContext op) throws SailException {
    }

    @Override
    public void clear(Resource... contexts) throws SailException {
        removeStatements(null, null, null, contexts); //remove all statements in the contexts.
    }

	public long clearGraph(UpdateContext op, Resource... contexts) throws SailException {
		return removeUnknownStatements(null, null, null, contexts, getUpdateTimestamp(op));
	}

	private void clearAllStatements() throws SailException {
        try {
			HalyardTableUtils.clearStatements(sail.hConnection, sail.tableName);
        } catch (IOException ex) {
            throw new SailException(ex);
        }
    }

	void addNamespaces() throws SailException {
		boolean nsExists = hasStatement(HALYARD.SYSTEM_GRAPH_CONTEXT, RDF.TYPE, SD.NAMED_GRAPH_CLASS, false, HALYARD.SYSTEM_GRAPH_CONTEXT);
		if (!nsExists) {
			for (Namespace ns : sail.getRDFFactory().getWellKnownNamespaces()) {
				setNamespace(ns.getPrefix(), ns.getName());
			}
			addSystemStatement(HALYARD.SYSTEM_GRAPH_CONTEXT, RDF.TYPE, SD.NAMED_GRAPH_CLASS, HALYARD.SYSTEM_GRAPH_CONTEXT, getDefaultTimestamp(false));
		}
	}

    @Override
    public String getNamespace(String prefix) throws SailException {
        ValueFactory vf = sail.getValueFactory();
		try (CloseableIteration<? extends Statement> nsIter = getStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), false, new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT })) {
    		if (nsIter.hasNext()) {
    			IRI namespace = (IRI) nsIter.next().getSubject();
    			return namespace.stringValue();
    		} else {
    			return null;
    		}
    	}
    }

    @Override
	public CloseableIteration<? extends Namespace> getNamespaces() {
		CloseableIteration<? extends Statement> nsIter = getStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, null, false, new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT });
		return new ConvertingIteration<Statement, Namespace>(nsIter) {
			@Override
			protected Namespace convert(Statement stmt)
				throws SailException {
                String name = stmt.getSubject().stringValue();
                String prefix = stmt.getObject().stringValue();
				return new SimpleNamespace(prefix, name);
			}
    	};
    }

    @Override
    public void setNamespace(String prefix, String name) throws SailException {
		checkWritable();
        ValueFactory vf = sail.getValueFactory();
		Literal prefixValue = vf.createLiteral(prefix);
		IRI namespaceIri = vf.createIRI(name);
		deleteSystemStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, prefixValue, new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT }, getDefaultTimestamp(true));
		addSystemStatement(namespaceIri, HALYARD.NAMESPACE_PREFIX_PROPERTY, prefixValue, HALYARD.SYSTEM_GRAPH_CONTEXT, getDefaultTimestamp(false));
    }

    @Override
    public void removeNamespace(String prefix) throws SailException {
		checkWritable();
        ValueFactory vf = sail.getValueFactory();
		long timestamp = getDefaultTimestamp(true);
		deleteSystemStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, vf.createLiteral(prefix), new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT }, timestamp);
    }

    @Override
    public void clearNamespaces() throws SailException {
		checkWritable();
		long timestamp = getDefaultTimestamp(true);
		deleteSystemStatements(null, HALYARD.NAMESPACE_PREFIX_PROPERTY, null, new Resource[] { HALYARD.SYSTEM_GRAPH_CONTEXT }, timestamp);
    }

	@Override
	public String toString() {
		return super.toString() + "[keyspace = " + keyspaceConn.toString() + "]";
	}


	static final class Factory implements SailConnectionFactory {
		static final SailConnectionFactory INSTANCE = new Factory();

		@Override
		public HBaseSailConnection createConnection(HBaseSail sail) throws IOException {
			HBaseSailConnection conn = new HBaseSailConnection(sail);
			conn.setTrackResultSize(sail.isTrackResultSize());
			conn.setTrackResultTime(sail.isTrackResultTime());
			conn.setTrackBranchOperatorsOnly(sail.isTrackBranchOperatorsOnly());
			return conn;
		}
	}

	@FunctionalInterface
	private static interface QueryEvaluator<E> {
		E evaluate(TupleExpr optimizedTree, QueryEvaluationStep step, HBaseSail.QueryInfo queryInfo);
	}

	private static String internalBinding(String name) {
		return INTERNAL_BINDING_PREFIX + name + INTERNAL_BINDING_SUFFIX;
	}

	private static boolean isInternalBinding(String name) {
		return name.startsWith(INTERNAL_BINDING_PREFIX) && name.endsWith(INTERNAL_BINDING_SUFFIX);
	}
}
