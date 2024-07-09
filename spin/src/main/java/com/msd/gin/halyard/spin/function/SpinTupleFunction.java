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

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;

import com.google.common.base.Joiner;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.QueryPreparer;
import com.msd.gin.halyard.query.algebra.evaluation.function.ExtendedTupleFunction;
import com.msd.gin.halyard.spin.Argument;
import com.msd.gin.halyard.spin.function.ConstructTupleFunction.GraphQueryResultIteration;
import com.msd.gin.halyard.spin.function.SelectTupleFunction.TupleQueryResultIteration;

public class SpinTupleFunction extends AbstractSpinFunction implements TransientTupleFunction, ExtendedTupleFunction {

	private ParsedQuery parsedQuery;

	private final List<Argument> arguments = new ArrayList<>(4);

	public SpinTupleFunction(String uri) {
		super(uri);
	}

	public void setParsedQuery(ParsedQuery query) {
		this.parsedQuery = query;
	}

	public ParsedQuery getParsedQuery() {
		return parsedQuery;
	}

	public void addArgument(Argument arg) {
		arguments.add(arg);
	}

	public List<Argument> getArguments() {
		return arguments;
	}

	@Override
	public String toString() {
		return getURI() + "(" + Joiner.on(", ").join(arguments) + ")";
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>> evaluate(
			TripleSource tripleSource, Value... args) throws QueryEvaluationException {
		ExtendedTripleSource extTripleSource = (ExtendedTripleSource) tripleSource;
		CloseableIteration<? extends List<? extends Value>> iter;
		if (parsedQuery instanceof ParsedBooleanQuery) {
			ParsedBooleanQuery askQuery = (ParsedBooleanQuery) parsedQuery;
			Value result;
			try (QueryPreparer qp = extTripleSource.newQueryPreparer()) {
				BooleanQuery queryOp = qp.prepare(askQuery);
				addBindings(queryOp, arguments, args);
				result = BooleanLiteral.valueOf(queryOp.evaluate());
			}
			iter = new SingletonIteration<>(Collections.singletonList(result));
		} else if (parsedQuery instanceof ParsedTupleQuery) {
			ParsedTupleQuery selectQuery = (ParsedTupleQuery) parsedQuery;
			QueryPreparer qp = extTripleSource.newQueryPreparer();
			TupleQuery queryOp = qp.prepare(selectQuery);
			addBindings(queryOp, arguments, args);
			iter = new TupleQueryResultIteration(qp, queryOp.evaluate());
		} else if (parsedQuery instanceof ParsedGraphQuery) {
			ParsedGraphQuery graphQuery = (ParsedGraphQuery) parsedQuery;
			QueryPreparer qp = extTripleSource.newQueryPreparer();
			GraphQuery queryOp = qp.prepare(graphQuery);
			addBindings(queryOp, arguments, args);
			iter = new GraphQueryResultIteration(qp, queryOp.evaluate());
		} else {
			throw new IllegalStateException("Unexpected query: " + parsedQuery);
		}
		return iter;
	}

	private static void addBindings(Query query, List<Argument> arguments, Value... args) {
		for (int i = 0; i < args.length; i++) {
			Argument argument = arguments.get(i);
			query.setBinding(argument.getPredicate().getLocalName(), args[i]);
		}
	}
}
