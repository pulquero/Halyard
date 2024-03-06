package com.msd.gin.halyard.client;

import java.io.IOException;
import java.util.List;

import org.apache.http.NameValuePair;
import org.eclipse.rdf4j.http.client.SPARQLProtocolSession;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.QueryStringUtil;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLTupleQuery;

public class ExtendedSPARQLTupleQuery extends SPARQLTupleQuery {

	private final List<NameValuePair> requestParams;

	public ExtendedSPARQLTupleQuery(SPARQLProtocolSession httpClient, String baseUri, String queryString, List<NameValuePair> requestParams) {
		super(httpClient, baseUri, queryString);
		if (!requestParams.isEmpty() && !(httpClient instanceof ExtendedSPARQLProtocolSession)) {
			throw new UnsupportedOperationException(String.format("%s does not support HTTP request parameters", httpClient.getClass().getName()));
		}
		this.requestParams = requestParams;
	}

	@Override
	public TupleQueryResult evaluate() throws QueryEvaluationException {

		SPARQLProtocolSession client = getHttpClient();
		if (client instanceof ExtendedSPARQLProtocolSession) {
			try {
				return ((ExtendedSPARQLProtocolSession)client).sendTupleQuery(QueryLanguage.SPARQL, getQueryString(), baseURI, dataset, getIncludeInferred(),
						getMaxExecutionTime(), requestParams, getBindingsArray());
			} catch (IOException | RepositoryException | MalformedQueryException e) {
				throw new QueryEvaluationException(e.getMessage(), e);
			}
		} else {
			return super.evaluate();
		}
	}

	@Override
	public void evaluate(TupleQueryResultHandler handler)
			throws QueryEvaluationException, TupleQueryResultHandlerException {
		SPARQLProtocolSession client = getHttpClient();
		if (client instanceof ExtendedSPARQLProtocolSession) {
			try {
				((ExtendedSPARQLProtocolSession)client).sendTupleQuery(QueryLanguage.SPARQL, getQueryString(), baseURI, dataset, getIncludeInferred(),
						getMaxExecutionTime(), handler, requestParams, getBindingsArray());
			} catch (IOException | RepositoryException | MalformedQueryException e) {
				throw new QueryEvaluationException(e.getMessage(), e);
			}
		} else {
			super.evaluate(handler);
		}
	}

	private String getQueryString() {
		return QueryStringUtil.getTupleQueryString(queryString, getBindings());
	}
}
