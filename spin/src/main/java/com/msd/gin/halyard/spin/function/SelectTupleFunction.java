/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package com.msd.gin.halyard.spin.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;

import com.msd.gin.halyard.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.algebra.evaluation.QueryPreparer;
import com.msd.gin.halyard.function.ExtendedTupleFunction;
import com.msd.gin.halyard.spin.SpinParser;

public class SelectTupleFunction extends AbstractSpinFunction implements ExtendedTupleFunction {

	private SpinParser parser;

	public SelectTupleFunction() {
		super(SPIN.SELECT_PROPERTY.stringValue());
	}

	public SelectTupleFunction(SpinParser parser) {
		this();
		this.parser = parser;
	}

	public SpinParser getSpinParser() {
		return parser;
	}

	public void setSpinParser(SpinParser parser) {
		this.parser = parser;
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(
			TripleSource tripleSource, Value... args) throws QueryEvaluationException {
		if (args.length == 0 || !(args[0] instanceof Resource)) {
			throw new QueryEvaluationException("First argument must be a resource");
		}
		if ((args.length % 2) == 0) {
			throw new QueryEvaluationException("Old number of arguments required");
		}
		ExtendedTripleSource extTripleSource = (ExtendedTripleSource) tripleSource;
		try {
			ParsedQuery parsedQuery = parser.parseQuery((Resource) args[0], extTripleSource);
			if (parsedQuery instanceof ParsedTupleQuery) {
				ParsedTupleQuery tupleQuery = (ParsedTupleQuery) parsedQuery;
				QueryPreparer qp = extTripleSource.newQueryPreparer();
				TupleQuery queryOp = qp.prepare(tupleQuery);
				addBindings(queryOp, args);
				final TupleQueryResult queryResult = queryOp.evaluate();
				return new TupleQueryResultIteration(qp, queryResult);
			} else if (parsedQuery instanceof ParsedBooleanQuery) {
				ParsedBooleanQuery booleanQuery = (ParsedBooleanQuery) parsedQuery;
				Value result;
				try (QueryPreparer qp = extTripleSource.newQueryPreparer()) {
					BooleanQuery queryOp = qp.prepare(booleanQuery);
					addBindings(queryOp, args);
					result = BooleanLiteral.valueOf(queryOp.evaluate());
				}
				return new SingletonIteration<>(Collections.singletonList(result));
			} else {
				throw new QueryEvaluationException("First argument must be a SELECT or ASK query");
			}
		} catch (QueryEvaluationException e) {
			throw e;
		} catch (RDF4JException e) {
			throw new ValueExprEvaluationException(e);
		}
	}

	static class TupleQueryResultIteration extends AbstractCloseableIteration<List<Value>, QueryEvaluationException> {
		private final QueryPreparer qp;

		private final TupleQueryResult queryResult;

		private final List<String> bindingNames;

		TupleQueryResultIteration(QueryPreparer qp, TupleQueryResult queryResult) throws QueryEvaluationException {
			this.qp = qp;
			this.queryResult = queryResult;
			this.bindingNames = queryResult.getBindingNames();
		}

		@Override
		public boolean hasNext() throws QueryEvaluationException {
			if (isClosed()) {
				return false;
			}
			boolean result = queryResult.hasNext();
			if (!result) {
				close();
			}
			return result;
		}

		@Override
		public List<Value> next() throws QueryEvaluationException {
			if (isClosed()) {
				throw new NoSuchElementException("The iteration has been closed.");
			}
			try {
				BindingSet bs = queryResult.next();
				List<Value> values = new ArrayList<>(bindingNames.size());
				for (String bindingName : bindingNames) {
					values.add(bs.getValue(bindingName));
				}
				return values;
			} catch (NoSuchElementException e) {
				close();
				throw e;
			}
		}

		@Override
		public void remove() throws QueryEvaluationException {
			if (isClosed()) {
				throw new IllegalStateException("The iteration has been closed.");
			}
			try {
				queryResult.remove();
			} catch (IllegalStateException e) {
				close();
				throw e;
			}
		}

		@Override
		public void handleClose() throws QueryEvaluationException {
			try {
				try {
					super.handleClose();
				} finally {
					queryResult.close();
				}
			} finally {
				qp.close();
			}
		}
	}
}
