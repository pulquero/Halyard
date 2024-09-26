package com.msd.gin.halyard.spin;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

import com.msd.gin.halyard.query.algebra.evaluation.CloseableTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.QueryPreparer;

class ExtendedTripleSourceWrapper implements ExtendedTripleSource, CloseableTripleSource {
	private final TripleSource delegate;
	private final QueryPreparer.Factory queryPreparerFactory;
	private final Map<Class<?>,?> queryHelpers;

	ExtendedTripleSourceWrapper(TripleSource delegate, QueryPreparer.Factory queryPreparerFactory, Map<Class<?>,?> queryHelpers) {
		this.delegate = delegate;
		this.queryPreparerFactory = queryPreparerFactory;
		// must be thread-safe
		this.queryHelpers = Collections.unmodifiableMap(new IdentityHashMap<>(queryHelpers));
	}

	@Override
	public QueryPreparer newQueryPreparer() {
		return queryPreparerFactory.create();
	}

	@Override
	public <T> T getQueryHelper(Class<T> qhType) {
		Object qh = queryHelpers.get(qhType);
		if (qh == null) {
			throw new QueryEvaluationException(String.format("%s is not available", qhType.getName()));
		}
		return qhType.cast(qh);
	}

	@Override
	public boolean hasQueryHelper(Class<?> qhType) {
		return queryHelpers.containsKey(qhType);
	}

	@Override
	public CloseableIteration<? extends Statement> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		return delegate.getStatements(subj, pred, obj, contexts);
	}

	@Override
	public ValueFactory getValueFactory() {
		return delegate.getValueFactory();
	}

	@Override
	public void close() {
		if (delegate instanceof CloseableTripleSource) {
			((CloseableTripleSource)delegate).close();
		}
	}

}