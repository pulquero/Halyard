package com.msd.gin.halyard.repository;

import com.google.common.base.Stopwatch;
import com.msd.gin.halyard.common.Timestamped;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.CloseableConsumer;
import com.msd.gin.halyard.query.TimeLimitConsumer;
import com.msd.gin.halyard.query.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;
import com.msd.gin.halyard.sail.TimestampedUpdateContext;
import com.msd.gin.halyard.spin.SpinMagicPropertyInterpreter;
import com.msd.gin.halyard.spin.SpinParser;
import com.msd.gin.halyard.strategy.StrategyConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.TimeLimitIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.query.algebra.Add;
import org.eclipse.rdf4j.query.algebra.Clear;
import org.eclipse.rdf4j.query.algebra.Copy;
import org.eclipse.rdf4j.query.algebra.Create;
import org.eclipse.rdf4j.query.algebra.DeleteData;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.InsertData;
import org.eclipse.rdf4j.query.algebra.Load;
import org.eclipse.rdf4j.query.algebra.Modify;
import org.eclipse.rdf4j.query.algebra.Move;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.StatementPattern.Scope;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.StatementPatternCollector;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailUpdate;
import org.eclipse.rdf4j.repository.sail.helpers.SailUpdateExecutor;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseUpdate extends SailUpdate implements Timestamped {
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseUpdate.class);

	private final HBaseSail sail;
	private long ts = NOT_SET;

	public HBaseUpdate(ParsedUpdate parsedUpdate, HBaseSail sail, SailRepositoryConnection con) {
		super(parsedUpdate, con);
		this.sail = sail;
	}

	@Override
	public long getTimestamp() {
		return ts;
	}

	@Override
	public void setTimestamp(long ts) {
		this.ts = ts;
	}

	@Override
	public void execute() throws UpdateExecutionException {
		ParsedUpdate parsedUpdate = getParsedUpdate();
		List<UpdateExpr> updateExprs = parsedUpdate.getUpdateExprs();
		Map<UpdateExpr, Dataset> datasetMapping = parsedUpdate.getDatasetMapping();

		SailRepositoryConnection con = getConnection();
		ValueFactory vf = con.getValueFactory();
		HBaseUpdateExecutor executor = new HBaseUpdateExecutor(sail, (HBaseSailConnection) con.getSailConnection(), vf, con.getParserConfig());

		boolean localTransaction = false;
		try {
			if (!getConnection().isActive()) {
				localTransaction = true;
				beginLocalTransaction();
			}
			for (int i = 0; i < updateExprs.size(); i++) {
				UpdateExpr updateExpr = updateExprs.get(i);
				Dataset activeDataset = getMergedDataset(datasetMapping.get(updateExpr));

				QueryBindingSet updateBindings = new QueryBindingSet(getBindings());
				updateBindings.addBinding(HBaseSailConnection.UPDATE_PART_BINDING, vf.createLiteral(i));
				try {
					executor.executeUpdate(updateExpr, activeDataset, updateBindings, getIncludeInferred(), getMaxExecutionTime(), getTimestamp());
				} catch (RDF4JException | IOException e) {
					LOGGER.warn("exception during update execution: ", e);
					if (!updateExpr.isSilent()) {
						throw new UpdateExecutionException(e);
					}
				}
			}

			if (localTransaction) {
				commitLocalTransaction();
				localTransaction = false;
			}
		} finally {
			if (localTransaction) {
				rollbackLocalTransaction();
			}
		}
	}

	private void beginLocalTransaction() throws RepositoryException {
		getConnection().begin();
	}

	private void commitLocalTransaction() throws RepositoryException {
		getConnection().commit();

	}

	private void rollbackLocalTransaction() throws RepositoryException {
		getConnection().rollback();

	}

	static class HBaseUpdateExecutor extends SailUpdateExecutor {
		final HBaseSail sail;
		final HBaseSailConnection con;
		final ValueFactory vf;

		public HBaseUpdateExecutor(HBaseSail sail, HBaseSailConnection con, ValueFactory vf, ParserConfig loadConfig) {
			super(con, vf, loadConfig);
			this.sail = sail;
			this.con = con;
			this.vf = vf;
		}

		public void executeUpdate(UpdateExpr updateExpr, Dataset dataset, BindingSet bindings, boolean includeInferred, int maxExecutionTime, long ts) throws SailException, RDFParseException, IOException {
			TimestampedUpdateContext uc = new TimestampedUpdateContext(updateExpr, dataset, bindings, includeInferred);
			uc.setTimestamp(ts);

			con.startUpdate(uc);
			try {
				if (updateExpr instanceof Load) {
					executeLoad((Load) updateExpr, uc);
				} else if (updateExpr instanceof Modify) {
					executeModify((Modify) updateExpr, uc, maxExecutionTime);
				} else if (updateExpr instanceof InsertData) {
					executeInsertData((InsertData) updateExpr, uc, maxExecutionTime);
				} else if (updateExpr instanceof DeleteData) {
					executeDeleteData((DeleteData) updateExpr, uc, maxExecutionTime);
				} else if (updateExpr instanceof Clear) {
					executeClear((Clear) updateExpr, uc, maxExecutionTime);
				} else if (updateExpr instanceof Create) {
					executeCreate((Create) updateExpr, uc);
				} else if (updateExpr instanceof Copy) {
					executeCopy((Copy) updateExpr, uc, maxExecutionTime);
				} else if (updateExpr instanceof Add) {
					executeAdd((Add) updateExpr, uc, maxExecutionTime);
				} else if (updateExpr instanceof Move) {
					executeMove((Move) updateExpr, uc, maxExecutionTime);
				} else if (updateExpr instanceof Load) {
					throw new SailException("load operations can not be handled directly by the SAIL");
				}
			} finally {
				con.endUpdate(uc);
			}
		}

		protected void executeModify(Modify modify, TimestampedUpdateContext uc, int maxExecutionTime) throws SailException {
			try {
				final InsertAction insertAction;
				TupleExpr insertClause = modify.getInsertExpr();
				if (insertClause != null) {
					// for inserts, TupleFunctions are expected in the insert clause
					insertClause = Algebra.ensureRooted(insertClause);
					insertClause = optimize(insertClause, uc.getDataset(), uc.getBindingSet(), false);
					InsertCollector insertCollector = new InsertCollector();
					insertClause.visit(insertCollector);
					insertAction = new InsertAction(sail, con, vf, uc, insertClause, insertCollector.getStatementPatterns(), insertCollector.getTupleFunctionCalls());
				} else {
					insertAction = null;
				}

				final DeleteAction deleteAction;
				TupleExpr deleteClause = modify.getDeleteExpr();
				TupleExpr whereClause = modify.getWhereExpr();
				whereClause = Algebra.ensureRooted(whereClause);
				if (deleteClause != null) {
					// for deletes, TupleFunctions are expected in the where clause
					whereClause = optimize(whereClause, uc.getDataset(), uc.getBindingSet(), true);
					deleteAction = new DeleteAction(sail, con, vf, uc, deleteClause, StatementPatternCollector.process(deleteClause), WhereCollector.process(whereClause));
				} else {
					deleteAction = null;
				}

				if (deleteAction != null) {
					deleteAction.resetResults();
				}
				if (insertAction != null) {
					insertAction.resetResults();
				}

				try (CloseableConsumer<BindingSet> callback = TimeLimitConsumer.apply(next -> {
					if (deleteAction != null) {
						deleteAction.deleteBoundTriples(next);
					}
					if (insertAction != null) {
						insertAction.insertBoundTriples(next);
					}
				}, maxExecutionTime)) {
					evaluateWhereClause(callback, whereClause, uc);
				}

				// copy final results back to original expressions
				if (deleteAction != null) {
					deleteAction.updateResults(true);
					deleteAction.copyResults(modify.getDeleteExpr());
				}
				if (insertAction != null) {
					insertAction.updateResults(true);
					insertAction.copyResults(modify.getInsertExpr());
				}
			} catch (QueryEvaluationException e) {
				throw new SailException(e);
			}
		}

		private TupleExpr optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeMatchingTriples) {
			LOGGER.debug("Update TupleExpr before interpretation:\n{}", tupleExpr);
			SpinParser spinParser = sail.getSpinParser();
			TripleSource source = new EmptyTripleSource(vf);
			new SpinMagicPropertyInterpreter(spinParser, source, sail.getTupleFunctionRegistry(), sail.getFederatedServiceResolver(), includeMatchingTriples).optimize(tupleExpr, dataset, bindings);
			LOGGER.debug("Update TupleExpr after interpretation:\n{}", tupleExpr);
			return tupleExpr;
		}

		private void evaluateWhereClause(Consumer<BindingSet> handler, final TupleExpr whereClause, final UpdateContext uc) {
			Consumer<BindingSet> ucHandler = new Consumer<BindingSet>() {
				private final boolean isEmptyWhere = Algebra.isEmpty(whereClause);
				private final BindingSet ucBinding = uc.getBindingSet();
				@Override
				public void accept(BindingSet sourceBinding) {
					final BindingSet bs;
					if (isEmptyWhere && sourceBinding.isEmpty() && ucBinding != null) {
						// in the case of an empty WHERE clause, we use the
						// supplied
						// bindings to produce triples to DELETE/INSERT
						bs = ucBinding;
					} else {
						// check if any supplied bindings do not occur in the
						// bindingset
						// produced by the WHERE clause. If so, merge.
						Set<String> uniqueBindings = new HashSet<>(ucBinding.getBindingNames());
						uniqueBindings.removeAll(sourceBinding.getBindingNames());
						if (!uniqueBindings.isEmpty()) {
							MapBindingSet mergedSet = new MapBindingSet(sourceBinding.size() + uniqueBindings.size());
							for (String bindingName : sourceBinding.getBindingNames()) {
								Value v = sourceBinding.getValue(bindingName);
								if (v != null) {
									mergedSet.addBinding(bindingName, v);
								}
							}
							for (String bindingName : uniqueBindings) {
								Value v = ucBinding.getValue(bindingName);
								if (v != null) {
									mergedSet.addBinding(bindingName, v);
								}
							}
							bs = mergedSet;
						} else {
							bs = sourceBinding;
						}
					}
					handler.accept(bs);
				}
			};
			con.evaluate(ucHandler, whereClause, uc.getDataset(), uc.getBindingSet(), uc.isIncludeInferred());
		}

		protected void executeClear(Clear clearExpr, TimestampedUpdateContext uc, int maxExecutionTime) throws SailException {
			try {
				ValueConstant graph = clearExpr.getGraph();

				long deletedCount;
				Stopwatch stopwatch;
				if (con.isTrackResultSize()) {
					Algebra._initResultSizeActual(clearExpr);
				}
				if (con.isTrackResultTime()) {
					Algebra._initTotalTimeNanosActual(clearExpr);
					stopwatch = Stopwatch.createStarted();
				} else {
					stopwatch = null;
				}
				if (graph != null) {
					Resource context = (Resource) graph.getValue();
					deletedCount = con.clearGraph(uc, context);
				} else {
					Scope scope = clearExpr.getScope();
					if (Scope.NAMED_CONTEXTS.equals(scope)) {
						CloseableIteration<? extends Resource, SailException> contextIDs = con.getContextIDs();
						try {
							if (maxExecutionTime > 0) {
								contextIDs = new TimeLimitIteration<Resource, SailException>(contextIDs, TimeUnit.SECONDS.toMillis(maxExecutionTime)) {

									@Override
									protected void throwInterruptedException() throws SailException {
										throw new SailException("execution took too long");
									}
								};
							}
							deletedCount = 0L;
							while (contextIDs.hasNext()) {
								deletedCount += con.clearGraph(uc, contextIDs.next());
							}
						} finally {
							contextIDs.close();
						}
					} else if (Scope.DEFAULT_CONTEXTS.equals(scope)) {
						deletedCount = con.clearGraph(uc, (Resource) null);
					} else {
						deletedCount = con.clearGraph(uc);
					}
				}
				if (con.isTrackResultTime()) {
					stopwatch.stop();
					Algebra._incrementTotalTimeNanosActual(clearExpr, stopwatch.elapsed(TimeUnit.NANOSECONDS));
				}
				if (con.isTrackResultSize()) {
					Algebra._incrementResultSizeActual(clearExpr, deletedCount);
				}
			} catch (SailException e) {
				if (!clearExpr.isSilent()) {
					throw e;
				}
			}
		}
	}

	static abstract class ModifyAction {
		private final StrategyConfig config;
		protected final HBaseSailConnection conn;
		protected final TupleExpr clause;
		private final List<StatementPattern> stPatterns;
		private final List<TupleFunctionCall> tupleFunctionCalls;
		protected long resultCounter;
		protected Stopwatch stopwatch;

		ModifyAction(HBaseSail sail, HBaseSailConnection conn, TupleExpr clause, List<StatementPattern> stPatterns, List<TupleFunctionCall> tupleFunctionCalls) {
			this.config = sail.getStrategyConfig();
			this.conn = conn;
			this.clause = clause;
			this.stPatterns = stPatterns;
			this.tupleFunctionCalls = tupleFunctionCalls;
		}

		public final TupleExpr getClause() {
			return clause;
		}

		public final List<StatementPattern> getStatementPatterns() {
			return stPatterns;
		}

		public final List<TupleFunctionCall> getTupleFunctionCalls() {
			return tupleFunctionCalls;
		}

		public final void resetResults() {
			if (conn.isTrackResultTime()) {
				Algebra._initTotalTimeNanosActual(clause);
				stopwatch = Stopwatch.createUnstarted();
			}
			if (conn.isTrackResultSize()) {
				Algebra._initResultSizeActual(clause);
				resultCounter = 0;
			}
		}

		public final void copyResults(TupleExpr expr) {
			if (conn.isTrackResultTime()) {
				expr.setTotalTimeNanosActual(Math.max(0, expr.getTotalTimeNanosActual()) + clause.getTotalTimeNanosActual());
			}
			if (conn.isTrackResultSize()) {
				expr.setResultSizeActual(Math.max(0, expr.getResultSizeActual()) + clause.getResultSizeActual());
			}
		}

		protected final void updateResults(boolean force) {
			if (conn.isTrackResultTime()) {
				long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
				if (force || elapsed > config.trackResultTimeUpdateInterval) {
					Algebra._incrementTotalTimeNanosActual(clause, elapsed);
					stopwatch.reset();
				}
			}
			if (conn.isTrackResultSize()) {
				if (force || resultCounter > config.trackResultSizeUpdateInterval) {
					Algebra._incrementResultSizeActual(clause, resultCounter);
					resultCounter = 0;
				}
			}
		}
	}

	static final class DeleteAction extends ModifyAction {
		List<StatementDeleter> spDeleters;

		DeleteAction(HBaseSail sail, HBaseSailConnection conn, ValueFactory vf, TimestampedUpdateContext uc, TupleExpr clause, List<StatementPattern> stPatterns, List<TupleFunctionCall> tupleFunctionCalls) {
			super(sail, conn, clause, stPatterns, tupleFunctionCalls);
			spDeleters = new ArrayList<>(stPatterns.size());
			for (StatementPattern sp : stPatterns) {
				spDeleters.add(new StatementDeleter(conn, vf, uc, sp, this));
			}
		}

		void deleteBoundTriples(BindingSet whereBinding) throws SailException {
			if (stopwatch != null) {
				stopwatch.start();
			}

			for (StatementDeleter spDelete : spDeleters) {
				if (spDelete.delete(whereBinding)) {
					resultCounter++;
				}
			}

			if (stopwatch != null) {
				stopwatch.stop();
			}
			updateResults(false);
		}
	}

	static final class StatementDeleter extends StatementModifier {
		static final IRI[] DEFAULT_GRAPH = new IRI[] { null };
		final StatementPattern deletePattern;
		final IRI[] defaultRemoveGraphs;

		StatementDeleter(HBaseSailConnection conn, ValueFactory vf, TimestampedUpdateContext uc, StatementPattern deletePattern, ModifyAction deleteInfo) {
			super(conn, vf, uc.getTimestamp(), deleteInfo.getTupleFunctionCalls());
			this.deletePattern = deletePattern;
			this.defaultRemoveGraphs = getDefaultRemoveGraphs(uc.getDataset());
		}

		boolean delete(BindingSet whereBinding) {
			Value patternValue = Algebra.getVarValue(deletePattern.getSubjectVar(), whereBinding);
			Resource subject = patternValue instanceof Resource ? (Resource) patternValue : null;

			patternValue = Algebra.getVarValue(deletePattern.getPredicateVar(), whereBinding);
			IRI predicate = patternValue instanceof IRI ? (IRI) patternValue : null;

			Value object = Algebra.getVarValue(deletePattern.getObjectVar(), whereBinding);

			Resource context = null;
			if (deletePattern.getContextVar() != null) {
				patternValue = Algebra.getVarValue(deletePattern.getContextVar(), whereBinding);
				context = patternValue instanceof Resource ? (Resource) patternValue : null;
			}

			if (subject == null || predicate == null || object == null) {
				/*
				 * skip removal of triple if any variable is unbound (may happen with optional patterns or if triple pattern forms illegal triple). See SES-1047 and #610.
				 */
				return false;
			}

			Statement toBeDeleted = (context != null) ? vf.createStatement(subject, predicate, object, context) : vf.createStatement(subject, predicate, object);
			long ts = getTimestamp(toBeDeleted, whereBinding);

			if (context != null) {
				if (RDF4J.NIL.equals(context) || SESAME.NIL.equals(context)) {
					return deleteStatement(subject, predicate, object, DEFAULT_GRAPH, ts);
				} else {
					return deleteStatement(subject, predicate, object, new Resource[] { context }, ts);
				}
			} else {
				return deleteStatement(subject, predicate, object, defaultRemoveGraphs, ts);
			}
		}

		private boolean deleteStatement(Resource s, IRI p, Value o, Resource[] ctxs, long ts) {
			boolean isNew = isNew(s, p, o, ctxs, ts);
			if (isNew) {
				conn.removeStatement(s, p, o, ctxs, ts);
			}
			return isNew;
		}

		private static IRI[] getDefaultRemoveGraphs(Dataset dataset) {
			if (dataset == null) {
				return new IRI[0];
			}
			Set<IRI> set = new HashSet<>(dataset.getDefaultRemoveGraphs());
			if (set.isEmpty()) {
				return new IRI[0];
			}
			if (set.remove(RDF4J.NIL) | set.remove(SESAME.NIL)) {
				set.add(null);
			}

			return set.toArray(new IRI[set.size()]);
		}
	}

	static final class InsertAction extends ModifyAction {
		List<StatementInserter> spInserters;

		InsertAction(HBaseSail sail, HBaseSailConnection conn, ValueFactory vf, TimestampedUpdateContext uc, TupleExpr clause, List<StatementPattern> stPatterns, List<TupleFunctionCall> tupleFunctionCalls) {
			super(sail, conn, clause, stPatterns, tupleFunctionCalls);
			spInserters = new ArrayList<>(stPatterns.size());
			for (StatementPattern sp : stPatterns) {
				spInserters.add(new StatementInserter(conn, vf, uc, sp, this));
			}
		}

		void insertBoundTriples(BindingSet whereBinding) throws SailException {
			if (stopwatch != null) {
				stopwatch.start();
			}

			// bnodes in the insert pattern are locally scoped for each
			// individual source binding.
			MapBindingSet bnodeMapping = new MapBindingSet();
			for (StatementInserter spInsert : spInserters) {
				if (spInsert.insert(whereBinding, bnodeMapping)) {
					resultCounter++;
				}
			}

			if (stopwatch != null) {
				stopwatch.stop();
			}
			updateResults(false);
		}
	}

	static final class StatementInserter extends StatementModifier {
		final StatementPattern insertPattern;
		final IRI defaultInsertGraph;

		StatementInserter(HBaseSailConnection conn, ValueFactory vf, TimestampedUpdateContext uc, StatementPattern insertPattern, ModifyAction insertInfo) {
			super(conn, vf, uc.getTimestamp(), insertInfo.getTupleFunctionCalls());
			this.insertPattern = insertPattern;
			this.defaultInsertGraph = uc.getDataset().getDefaultInsertGraph();
		}

		boolean insert(BindingSet whereBinding, MapBindingSet bnodeMapping) {
			Statement toBeInserted = createStatementFromPattern(insertPattern, whereBinding, bnodeMapping);

			if (toBeInserted != null) {
				long ts = getTimestamp(toBeInserted, whereBinding);

				if (defaultInsertGraph == null && toBeInserted.getContext() == null) {
					return insertStatement(toBeInserted.getSubject(), toBeInserted.getPredicate(), toBeInserted.getObject(), null, ts);
				} else if (toBeInserted.getContext() == null) {
					return insertStatement(toBeInserted.getSubject(), toBeInserted.getPredicate(), toBeInserted.getObject(), defaultInsertGraph, ts);
				} else {
					return insertStatement(toBeInserted.getSubject(), toBeInserted.getPredicate(), toBeInserted.getObject(), toBeInserted.getContext(), ts);
				}
			}
			return false;
		}

		private boolean insertStatement(Resource s, IRI p, Value o, Resource ctx, long ts) {
			boolean isNew = isNew(s, p, o, new Resource[] { ctx }, ts);
			if (isNew) {
				conn.addStatement(s, p, o, ctx, ts);
			}
			return isNew;
		}

		private Statement createStatementFromPattern(StatementPattern pattern, BindingSet sourceBinding, MapBindingSet bnodeMapping) throws SailException {
			Resource subject = null;
			IRI predicate = null;
			Value object = null;
			Resource context = null;

			Value patternValue;
			if (pattern.getSubjectVar().hasValue()) {
				patternValue = pattern.getSubjectVar().getValue();
				if (patternValue instanceof Resource) {
					subject = (Resource) patternValue;
				}
			} else {
				patternValue = sourceBinding.getValue(pattern.getSubjectVar().getName());
				if (patternValue instanceof Resource) {
					subject = (Resource) patternValue;
				}

				if (subject == null && pattern.getSubjectVar().isAnonymous()) {
					Binding mappedSubject = bnodeMapping.getBinding(pattern.getSubjectVar().getName());

					if (mappedSubject != null) {
						patternValue = mappedSubject.getValue();
						if (patternValue instanceof Resource) {
							subject = (Resource) patternValue;
						}
					} else {
						subject = vf.createBNode();
						bnodeMapping.addBinding(pattern.getSubjectVar().getName(), subject);
					}
				}
			}

			if (subject == null) {
				return null;
			}

			if (pattern.getPredicateVar().hasValue()) {
				patternValue = pattern.getPredicateVar().getValue();
				if (patternValue instanceof IRI) {
					predicate = (IRI) patternValue;
				}
			} else {
				patternValue = sourceBinding.getValue(pattern.getPredicateVar().getName());
				if (patternValue instanceof IRI) {
					predicate = (IRI) patternValue;
				}
			}

			if (predicate == null) {
				return null;
			}

			if (pattern.getObjectVar().hasValue()) {
				object = pattern.getObjectVar().getValue();
			} else {
				object = sourceBinding.getValue(pattern.getObjectVar().getName());

				if (object == null && pattern.getObjectVar().isAnonymous()) {
					Binding mappedObject = bnodeMapping.getBinding(pattern.getObjectVar().getName());

					if (mappedObject != null) {
						patternValue = mappedObject.getValue();
						if (patternValue instanceof Resource) {
							object = (Resource) patternValue;
						}
					} else {
						object = vf.createBNode();
						bnodeMapping.addBinding(pattern.getObjectVar().getName(), object);
					}
				}
			}

			if (object == null) {
				return null;
			}

			if (pattern.getContextVar() != null) {
				if (pattern.getContextVar().hasValue()) {
					patternValue = pattern.getContextVar().getValue();
					if (patternValue instanceof Resource) {
						context = (Resource) patternValue;
					}
				} else {
					patternValue = sourceBinding.getValue(pattern.getContextVar().getName());
					if (patternValue instanceof Resource) {
						context = (Resource) patternValue;
					}
				}
			}

			Statement st;
			if (context != null) {
				st = vf.createStatement(subject, predicate, object, context);
			} else {
				st = vf.createStatement(subject, predicate, object);
			}
			return st;
		}
	}

	static abstract class StatementModifier {
		final HBaseSailConnection conn;
		final ValueFactory vf;
		final List<TupleFunctionCall> timestampTfcs;
		final long defaultTimestamp;
		Resource prevSubj;
		IRI prevPred;
		Value prevObj;
		Resource[] prevCtxs;
		long prevTimestamp;

		StatementModifier(HBaseSailConnection conn, ValueFactory vf, long initialTimestamp, List<TupleFunctionCall> tfcs) {
			this.conn = conn;
			this.vf = vf;
			this.defaultTimestamp = initialTimestamp;
			timestampTfcs = new ArrayList<>(tfcs.size());
			for (TupleFunctionCall tfc : tfcs) {
				if (HALYARD.TIMESTAMP_PROPERTY.stringValue().equals(tfc.getURI())) {
					timestampTfcs.add(tfc);
				}
			}
		}

		final boolean isNew(Resource s, IRI p, Value o, Resource[] ctxs, long ts) {
			if (s.equals(prevSubj) && p.equals(prevPred) && o.equals(prevObj) && Arrays.equals(ctxs, prevCtxs) && ts == prevTimestamp) {
				return false;
			}
			prevSubj = s;
			prevPred = p;
			prevObj = o;
			prevCtxs = ctxs;
			prevTimestamp = ts;
			return true;
		}

		final long getTimestamp(Statement stmt, BindingSet bindings) {
			for (TupleFunctionCall tfc : timestampTfcs) {
				List<ValueExpr> args = tfc.getArgs();
				Resource tsSubj = (Resource) Algebra.getVarValue((Var) args.get(0), bindings);
				IRI tsPred = (IRI) Algebra.getVarValue((Var) args.get(1), bindings);
				Value tsObj = Algebra.getVarValue((Var) args.get(2), bindings);
				Statement tsStmt;
				if (args.size() == 3) {
					tsStmt = vf.createStatement(tsSubj, tsPred, tsObj);
				} else if (args.size() == 4) {
					Resource tsCtx = (Resource) Algebra.getVarValue((Var) args.get(3), bindings);
					tsStmt = vf.createStatement(tsSubj, tsPred, tsObj, tsCtx);
				} else {
					tsStmt = null;
				}
				if (stmt.equals(tsStmt)) {
					Literal ts = (Literal) Algebra.getVarValue(tfc.getResultVars().get(0), bindings);
					if (XSD.DATETIME.equals(ts.getDatatype())) {
						return ts.calendarValue().toGregorianCalendar().getTimeInMillis();
					} else {
						return ts.longValue();
					}
				}
			}
			return defaultTimestamp;
		}
	}

	static final class InsertCollector extends StatementPatternCollector {
		private final List<TupleFunctionCall> tupleFunctionCalls = new ArrayList<>();

		public List<TupleFunctionCall> getTupleFunctionCalls() {
			return tupleFunctionCalls;
		}

		@Override
		public void meetOther(QueryModelNode node) {
			if (node instanceof TupleFunctionCall) {
				meet((TupleFunctionCall) node);
			} else {
				super.meetOther(node);
			}
		}

		public void meet(TupleFunctionCall tfc) {
			tupleFunctionCalls.add(tfc);
		}
	}

	static final class WhereCollector extends AbstractExtendedQueryModelVisitor<RuntimeException> {
		private final List<TupleFunctionCall> tupleFunctionCalls = new ArrayList<>();

		static List<TupleFunctionCall> process(TupleExpr expr) {
			WhereCollector whereCollector = new WhereCollector();
			expr.visit(whereCollector);
			return whereCollector.getTupleFunctionCalls();
		}

		public List<TupleFunctionCall> getTupleFunctionCalls() {
			return tupleFunctionCalls;
		}

		@Override
		public void meet(Filter node) {
			// Skip boolean constraints
			node.getArg().visit(this);
		}

		@Override
		public void meet(StatementPattern node) {
			// skip
		}

		@Override
		public void meetOther(QueryModelNode node) {
			if (node instanceof TupleFunctionCall) {
				meet((TupleFunctionCall) node);
			} else {
				super.meetOther(node);
			}
		}

		public void meet(TupleFunctionCall tfc) {
			tupleFunctionCalls.add(tfc);
			if (HALYARD.TIMESTAMP_PROPERTY.stringValue().equals(tfc.getURI())) {
				Algebra.remove(tfc);
			}
		}
	}

}
