package com.msd.gin.halyard.client;

import java.io.IOException;
import java.util.List;

import org.apache.http.NameValuePair;
import org.eclipse.rdf4j.http.client.SPARQLProtocolSession;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.QueryStringUtil;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLGraphQuery;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

public class ExtendedSPARQLGraphQuery extends SPARQLGraphQuery {

	private final List<NameValuePair> requestParams;

	public ExtendedSPARQLGraphQuery(SPARQLProtocolSession httpClient, String baseURI, String queryString, List<NameValuePair> requestParams) {
		super(httpClient, baseURI, queryString);
		if (!requestParams.isEmpty() && !(httpClient instanceof ExtendedSPARQLProtocolSession)) {
			throw new UnsupportedOperationException(String.format("%s does not support HTTP request parameters", httpClient.getClass().getName()));
		}
		this.requestParams = requestParams;
	}

	@Override
	public GraphQueryResult evaluate() throws QueryEvaluationException {
		SPARQLProtocolSession client = getHttpClient();
		if (client instanceof ExtendedSPARQLProtocolSession) {
			try {
				// TODO getQueryString() already inserts bindings, use emptybindingset
				// as last argument?
				return ((ExtendedSPARQLProtocolSession)client).sendGraphQuery(queryLanguage, getQueryString(), baseURI, dataset, getIncludeInferred(),
						getMaxExecutionTime(), requestParams, getBindingsArray());
			} catch (IOException | RepositoryException | MalformedQueryException e) {
				throw new QueryEvaluationException(e.getMessage(), e);
			}
		} else {
			return super.evaluate();
		}
	}

	@Override
	public void evaluate(RDFHandler handler) throws QueryEvaluationException, RDFHandlerException {

		SPARQLProtocolSession client = getHttpClient();
		if (client instanceof ExtendedSPARQLProtocolSession) {
			try {
				((ExtendedSPARQLProtocolSession)client).sendGraphQuery(queryLanguage, getQueryString(), baseURI, dataset, getIncludeInferred(),
						getMaxExecutionTime(), handler, requestParams, getBindingsArray());
			} catch (IOException | RepositoryException | MalformedQueryException e) {
				throw new QueryEvaluationException(e.getMessage(), e);
			}
		} else {
			super.evaluate(handler);
		}
	}

	private String getQueryString() {
		return QueryStringUtil.getGraphQueryString(queryString, getBindings());
	}
}
