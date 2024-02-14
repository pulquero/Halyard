package com.msd.gin.halyard.federation;

import com.msd.gin.halyard.algebra.ServiceRoot;
import com.msd.gin.halyard.common.ValueFactories;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.sail.BindingSetConsumerSail;
import com.msd.gin.halyard.sail.BindingSetConsumerSailConnection;
import com.msd.gin.halyard.sail.BindingSetPipeSail;
import com.msd.gin.halyard.sail.BindingSetPipeSailConnection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SilentIteration;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.repository.sparql.federation.JoinExecutorBase;
import org.eclipse.rdf4j.repository.sparql.query.InsertBindingSetCursor;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;

public class SailFederatedService implements BindingSetConsumerFederatedService, BindingSetPipeFederatedService {
	private final Sail sail;
	private volatile boolean isInit;

	public SailFederatedService(Sail sail) {
		this.sail = sail;
	}

	public Sail getSail() {
		return sail;
	}

	@Override
	public void initialize() throws QueryEvaluationException {
		if (!isInit) {
			synchronized (this) {
				if (!isInit) {
					sail.init();
					isInit = true;
				}
			}
		}
	}

	@Override
	public boolean isInitialized() {
		return isInit;
	}

	@Override
	public void shutdown() throws QueryEvaluationException {
		if (isInit) {
			synchronized (this) {
				if (isInit) {
					sail.shutDown();
					isInit = false;
				}
			}
		}
	}

	protected SailConnection getConnection() {
		return sail.getConnection();
	}

	@Override
	public boolean ask(Service service, BindingSet bindings, String baseUri) throws QueryEvaluationException {
		try (SailConnection conn = getConnection()) {
			try (CloseableIteration<? extends BindingSet, QueryEvaluationException> res = evaluateInternal((expr, bs) -> conn.evaluate(expr, null, bs, true), service, null, bindings, baseUri)) {
				return res.hasNext();
			}
		}
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> projectionVars,
			BindingSet bindings, String baseUri) throws QueryEvaluationException {
		SailConnection conn = getConnection();
		CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = evaluateInternal((expr, bs) -> conn.evaluate(expr, null, bs, true), service, projectionVars, bindings, baseUri);
		CloseableIteration<BindingSet, QueryEvaluationException> result = new InsertBindingSetCursor((CloseableIteration<BindingSet, QueryEvaluationException>) iter, bindings);
		result = new CloseConnectionIteration(result, conn);
		if (service.isSilent()) {
			result = new SilentIteration<>(result);
		}
		return result;
	}

	@Override
	public void select(Consumer<BindingSet> handler, Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
		if (sail instanceof BindingSetConsumerSail) {
			try (BindingSetConsumerSailConnection conn = (BindingSetConsumerSailConnection) getConnection()) {
				Consumer<BindingSet> serviceHandler = new InsertBindingSetCallback(handler, bindings);
				evaluateInternal((expr, bs) -> {conn.evaluate(serviceHandler, expr, null, bs, true); return null;}, service, projectionVars, bindings, baseUri);
			}
		} else {
			try (CloseableIteration<BindingSet, QueryEvaluationException> result = select(service, projectionVars, bindings, baseUri)) {
				BindingSetConsumerSailConnection.report(result, handler);
			}
		}
	}

	@Override
	public void select(BindingSetPipe pipe, Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
		if (sail instanceof BindingSetPipeSail) {
			try (BindingSetPipeSailConnection conn = (BindingSetPipeSailConnection) getConnection()) {
				BindingSetPipe servicePipe = new InsertBindingSetPipe(pipe, bindings);
				evaluateInternal((expr, bs) -> {conn.evaluate(servicePipe, expr, null, bs, true); return null;}, service, projectionVars, bindings, baseUri);
			}
		} else {
			try (CloseableIteration<BindingSet, QueryEvaluationException> result = select(service, projectionVars, bindings, baseUri)) {
				BindingSetPipeSailConnection.report(result, pipe);
				pipe.close();
			}
		}
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service,
			CloseableIteration<BindingSet, QueryEvaluationException> bindings, String baseUri)
			throws QueryEvaluationException {
		List<BindingSet> allBindings = new ArrayList<>();
		while (bindings.hasNext()) {
			allBindings.add(bindings.next());
		}

		if (allBindings.isEmpty()) {
			return new EmptyIteration<>();
		}

		Set<String> projectionVars = new HashSet<>(service.getServiceVars());
		CloseableIteration<BindingSet, QueryEvaluationException> result = new SimpleServiceIteration(allBindings, b -> select(service, projectionVars, b, baseUri));
		if (service.isSilent()) {
			result = new SilentIteration<>(result);
		}
		return result;
	}

	protected <E> E evaluateInternal(ServiceEvaluator<E> evaluator, Service service, Set<String> projectionVars, BindingSet bindings, String baseUri) {
		return evaluator.evaluate(ServiceRoot.create(service), createBindings(service, projectionVars, bindings));
	}

	protected MutableBindingSet createBindings(Service service, Set<String> projectionVars, BindingSet bindings) {
		return ValueFactories.convertValues(bindings, sail.getValueFactory());
	}

	@FunctionalInterface
	private static interface ServiceEvaluator<E> {
		E evaluate(TupleExpr tupleExpr, BindingSet bindings);
	}

	private static class SimpleServiceIteration extends JoinExecutorBase<BindingSet> {

		private final List<BindingSet> allBindings;
		private final Function<BindingSet,CloseableIteration<BindingSet, QueryEvaluationException>> selectEvaluator;

		public SimpleServiceIteration(List<BindingSet> allBindings, Function<BindingSet,CloseableIteration<BindingSet, QueryEvaluationException>> selectEvaluator) {
			super(null, null, null);
			this.allBindings = allBindings;
			this.selectEvaluator = selectEvaluator;
			run();
		}

		@Override
		protected void handleBindings() throws Exception {
			for (BindingSet b : allBindings) {
				addResult(selectEvaluator.apply(b));
			}
		}
	}


	private static class CloseConnectionIteration implements CloseableIteration<BindingSet, QueryEvaluationException> {
		private final CloseableIteration<BindingSet, QueryEvaluationException> delegate;
		private final SailConnection conn;

		CloseConnectionIteration(CloseableIteration<BindingSet, QueryEvaluationException> delegate, SailConnection conn) {
			this.delegate = delegate;
			this.conn = conn;
		}

		@Override
		public boolean hasNext() throws QueryEvaluationException {
			return delegate.hasNext();
		}

		@Override
		public BindingSet next() throws QueryEvaluationException {
			return delegate.next();
		}

		@Override
		public void remove() throws QueryEvaluationException {
			delegate.remove();
		}

		@Override
		public void close() throws QueryEvaluationException {
			try {
				delegate.close();
			} finally {
				conn.close();
			}
		}
	}


	private static class InsertBindingSetCallback implements Consumer<BindingSet> {
		private final Consumer<BindingSet> delegate;
		private final BindingSet bindingSet;

		InsertBindingSetCallback(Consumer<BindingSet> delegate, BindingSet bs) {
			this.delegate = delegate;
			this.bindingSet = bs;
		}

		@Override
		public void accept(BindingSet bs) {
			int size = bindingSet.size() + bs.size();
			QueryBindingSet combined = new QueryBindingSet(size);
			combined.addAll(bindingSet);
			for (Binding binding : bs) {
				combined.setBinding(binding);
			}
			delegate.accept(combined);
		}
	}


	private static class InsertBindingSetPipe extends BindingSetPipe {
		private final BindingSet bindingSet;

		InsertBindingSetPipe(BindingSetPipe delegate, BindingSet bs) {
			super(delegate);
			this.bindingSet = bs;
		}

		@Override
		protected boolean next(BindingSet bs) {
			int size = bindingSet.size() + bs.size();
			QueryBindingSet combined = new QueryBindingSet(size);
			combined.addAll(bindingSet);
			for (Binding binding : bs) {
				combined.setBinding(binding);
			}
			return parent.push(combined);
		}
	}
}
