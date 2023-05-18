/**
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.msd.gin.halyard.spin;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

import com.msd.gin.halyard.algebra.evaluation.CloseableTripleSource;
import com.msd.gin.halyard.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.algebra.evaluation.QueryPreparer;

class ExtendedTripleSourceWrapper implements ExtendedTripleSource, CloseableTripleSource {
	private final TripleSource delegate;
	private final QueryPreparer.Factory queryPreparerFactory;

	ExtendedTripleSourceWrapper(TripleSource delegate, QueryPreparer.Factory queryPreparerFactory) {
		this.delegate = delegate;
		this.queryPreparerFactory = queryPreparerFactory;
	}

	@Override
	public QueryPreparer newQueryPreparer() {
		return queryPreparerFactory.create();
	}

	@Override
	public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
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