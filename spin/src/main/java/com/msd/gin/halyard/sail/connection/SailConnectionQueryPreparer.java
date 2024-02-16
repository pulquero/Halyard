/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package com.msd.gin.halyard.sail.connection;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.sail.SailConnection;

import com.msd.gin.halyard.query.algebra.evaluation.QueryPreparer;

/**
 * QueryPreparer for use with Sails.
 */
public class SailConnectionQueryPreparer implements QueryPreparer {

	private final SailConnection con;

	private final boolean includeInferred;

	private final ValueFactory vf;

	private ParserConfig parserConfig = new ParserConfig();

	public SailConnectionQueryPreparer(SailConnection con, boolean includeInferred, ValueFactory vf) {
		this.con = con;
		this.includeInferred = includeInferred;
		this.vf = vf;
	}

	public void setParserConfig(ParserConfig parserConfig) {
		this.parserConfig = parserConfig;
	}

	public ParserConfig getParserConfig() {
		return parserConfig;
	}

	@Override
	public BooleanQuery prepare(ParsedBooleanQuery askQuery) {
		BooleanQuery query = new SailConnectionBooleanQuery(askQuery, con);
		query.setIncludeInferred(includeInferred);
		return query;
	}

	@Override
	public TupleQuery prepare(ParsedTupleQuery tupleQuery) {
		TupleQuery query = new SailConnectionTupleQuery(tupleQuery, con);
		query.setIncludeInferred(includeInferred);
		return query;
	}

	@Override
	public GraphQuery prepare(ParsedGraphQuery graphQuery) {
		GraphQuery query = new SailConnectionGraphQuery(graphQuery, con, vf);
		query.setIncludeInferred(includeInferred);
		return query;
	}

	@Override
	public Update prepare(ParsedUpdate graphUpdate) {
		Update update = new SailConnectionUpdate(graphUpdate, con, vf, parserConfig);
		update.setIncludeInferred(includeInferred);
		return update;
	}

	@Override
	public void close() {
		con.close();
	}
}
