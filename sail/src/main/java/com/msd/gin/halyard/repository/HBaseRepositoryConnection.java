package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.query.algebra.ExtendedQueryRoot;
import com.msd.gin.halyard.queryparser.SPARQLParser;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;

import java.util.ArrayList;
import java.util.Optional;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Operation;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.AbstractParserQuery;
import org.eclipse.rdf4j.query.impl.AbstractParserUpdate;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailBooleanQuery;
import org.eclipse.rdf4j.repository.sail.SailGraphQuery;
import org.eclipse.rdf4j.repository.sail.SailQuery;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;
import org.eclipse.rdf4j.sail.SailException;

public class HBaseRepositoryConnection extends SailRepositoryConnection {
	private final HBaseSail sail;

	protected HBaseRepositoryConnection(HBaseRepository repository, HBaseSailConnection sailConnection) {
		super(repository, sailConnection);
		this.sail = (HBaseSail) repository.getSail();
	}

	private void addImplicitBindings(Operation op) {
		ValueFactory vf = getValueFactory();
		String sourceString;
		if (op instanceof AbstractParserQuery) {
			sourceString = ((AbstractParserQuery) op).getParsedQuery().getSourceString();
		} else if (op instanceof AbstractParserUpdate) {
			sourceString = ((AbstractParserUpdate) op).getParsedUpdate().getSourceString();
		} else {
			sourceString = null;
		}
		if (sourceString != null) {
			op.setBinding(HBaseSailConnection.SOURCE_STRING_BINDING, vf.createLiteral(sourceString));
		}
	}

	private void swapRoot(ParsedQuery q) {
		TupleExpr root = q.getTupleExpr();
		if (root.getClass() == QueryRoot.class) {
			TupleExpr tree = ((QueryRoot) root).getArg();
			q.setTupleExpr(new ExtendedQueryRoot(tree));
		}
	}

	private SailQuery prepareQuery(String queryString, String baseURI) throws MalformedQueryException {
		ParsedQuery parsedQuery = SPARQLParser.parseQuery(queryString, baseURI, getValueFactory());

		if (parsedQuery instanceof ParsedTupleQuery) {
			Optional<TupleExpr> sailTupleExpr = getSailConnection().prepareQuery(QueryLanguage.SPARQL, Query.QueryType.TUPLE, queryString, baseURI);
			if (sailTupleExpr.isPresent()) {
				parsedQuery = new ParsedTupleQuery(queryString, sailTupleExpr.get());
			}
			return new SailTupleQuery((ParsedTupleQuery) parsedQuery, this);
		} else if (parsedQuery instanceof ParsedGraphQuery) {
			Optional<TupleExpr> sailTupleExpr = getSailConnection().prepareQuery(QueryLanguage.SPARQL, Query.QueryType.GRAPH, queryString, baseURI);
			if (sailTupleExpr.isPresent()) {
				parsedQuery = new ParsedGraphQuery(queryString, sailTupleExpr.get());
			}
			return new ExtSailGraphQuery((ParsedGraphQuery) parsedQuery, this);
		} else if (parsedQuery instanceof ParsedBooleanQuery) {
			Optional<TupleExpr> sailTupleExpr = getSailConnection().prepareQuery(QueryLanguage.SPARQL, Query.QueryType.BOOLEAN, queryString, baseURI);
			if (sailTupleExpr.isPresent()) {
				parsedQuery = new ParsedBooleanQuery(queryString, sailTupleExpr.get());
			}
			return new ExtSailBooleanQuery((ParsedBooleanQuery) parsedQuery, this);
		} else {
			throw new RuntimeException("Unexpected query type: " + parsedQuery.getClass());
		}
	}

	@Override
	public SailQuery prepareQuery(QueryLanguage ql, String queryString, String baseURI) throws MalformedQueryException {
		SailQuery query;
		if (QueryLanguage.SPARQL.equals(ql)) {
			query = prepareQuery(queryString, baseURI);
		} else {
			query = super.prepareQuery(ql, queryString, baseURI);
		}
		swapRoot(query.getParsedQuery());
		addImplicitBindings(query);
		return query;
	}

	private SailTupleQuery prepareTupleQuery(String queryString, String baseURI) throws MalformedQueryException {
		Optional<TupleExpr> sailTupleExpr = getSailConnection().prepareQuery(QueryLanguage.SPARQL, Query.QueryType.TUPLE, queryString, baseURI);
		ParsedTupleQuery parsedQuery = sailTupleExpr.map(expr -> new ParsedTupleQuery(queryString, expr)).orElse(SPARQLParser.parseTupleQuery(queryString, baseURI, getValueFactory()));
		return new ExtSailTupleQuery(parsedQuery, this);
	}

	@Override
	public SailTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString, String baseURI) throws MalformedQueryException {
		SailTupleQuery query;
		if (QueryLanguage.SPARQL.equals(ql)) {
			query = prepareTupleQuery(queryString, baseURI);
		} else {
			query = super.prepareTupleQuery(ql, queryString, baseURI);
		}
		swapRoot(query.getParsedQuery());
		addImplicitBindings(query);
		return query;
	}

	private SailGraphQuery prepareGraphQuery(String queryString, String baseURI) throws MalformedQueryException {
		Optional<TupleExpr> sailTupleExpr = getSailConnection().prepareQuery(QueryLanguage.SPARQL, Query.QueryType.GRAPH, queryString, baseURI);
		ParsedGraphQuery parsedQuery = sailTupleExpr.map(expr -> new ParsedGraphQuery(queryString, expr)).orElse(SPARQLParser.parseGraphQuery(queryString, baseURI, getValueFactory()));
		return new ExtSailGraphQuery(parsedQuery, this);
	}

	@Override
	public SailGraphQuery prepareGraphQuery(QueryLanguage ql, String queryString, String baseURI) throws MalformedQueryException {
		SailGraphQuery query;
		if (QueryLanguage.SPARQL.equals(ql)) {
			query = prepareGraphQuery(queryString, baseURI);
		} else {
			query = super.prepareGraphQuery(ql, queryString, baseURI);
		}
		swapRoot(query.getParsedQuery());
		addImplicitBindings(query);
		return query;
	}

	private SailBooleanQuery prepareBooleanQuery(String queryString, String baseURI) throws MalformedQueryException {
		Optional<TupleExpr> sailTupleExpr = getSailConnection().prepareQuery(QueryLanguage.SPARQL, Query.QueryType.BOOLEAN, queryString, baseURI);
		ParsedBooleanQuery parsedQuery = sailTupleExpr.map(expr -> new ParsedBooleanQuery(queryString, expr)).orElse(SPARQLParser.parseBooleanQuery(queryString, baseURI, getValueFactory()));
		return new ExtSailBooleanQuery(parsedQuery, this);
	}

	@Override
	public SailBooleanQuery prepareBooleanQuery(QueryLanguage ql, String queryString, String baseURI) throws MalformedQueryException {
		SailBooleanQuery query;
		if (QueryLanguage.SPARQL.equals(ql)) {
			query = prepareBooleanQuery(queryString, baseURI);
		} else {
			query = super.prepareBooleanQuery(ql, queryString, baseURI);
		}
		swapRoot(query.getParsedQuery());
		addImplicitBindings(query);
		return query;
	}

	@Override
	public HBaseUpdate prepareUpdate(String update) throws RepositoryException, MalformedQueryException {
		return prepareUpdate(QueryLanguage.SPARQL, update, null);
	}

	@Override
	public HBaseUpdate prepareUpdate(QueryLanguage ql, String updateQuery, String baseURI) throws RepositoryException, MalformedQueryException {
		ParsedUpdate parsedUpdate = SPARQLParser.parseUpdate(updateQuery, baseURI, getValueFactory());

		HBaseUpdate update = new HBaseUpdate(parsedUpdate, sail, this);
		addImplicitBindings(update);
		return update;
	}


	static final class ExtSailBooleanQuery extends SailBooleanQuery {
		protected ExtSailBooleanQuery(ParsedBooleanQuery booleanQuery, SailRepositoryConnection sailConnection) {
			super(booleanQuery, sailConnection);
		}
	}

	static final class ExtSailTupleQuery extends SailTupleQuery {
		protected ExtSailTupleQuery(ParsedTupleQuery tupleQuery, SailRepositoryConnection sailConnection) {
			super(tupleQuery, sailConnection);
		}

		@Override
		public void evaluate(TupleQueryResultHandler handler) throws QueryEvaluationException, TupleQueryResultHandlerException {
			TupleExpr tupleExpr = getParsedQuery().getTupleExpr();
			try {
				HBaseSailConnection sailCon = (HBaseSailConnection) getConnection().getSailConnection();
				handler.startQueryResult(new ArrayList<>(tupleExpr.getBindingNames()));
				sailCon.evaluate(handler::handleSolution, tupleExpr, getActiveDataset(), getBindings(), getIncludeInferred());
				handler.endQueryResult();
			} catch (SailException e) {
				throw new QueryEvaluationException(e.getMessage(), e);
			}
		}
	}

	static final class ExtSailGraphQuery extends SailGraphQuery {
		protected ExtSailGraphQuery(ParsedGraphQuery graphQuery, SailRepositoryConnection sailConnection) {
			super(graphQuery, sailConnection);
		}
	}
}
