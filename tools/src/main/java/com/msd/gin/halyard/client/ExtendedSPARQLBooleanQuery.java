package com.msd.gin.halyard.client;

import java.io.IOException;
import java.util.List;

import org.apache.http.NameValuePair;
import org.eclipse.rdf4j.http.client.SPARQLProtocolSession;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.QueryStringUtil;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLBooleanQuery;

public class ExtendedSPARQLBooleanQuery extends SPARQLBooleanQuery {

	private final List<NameValuePair> requestParams;

	public ExtendedSPARQLBooleanQuery(SPARQLProtocolSession httpClient, String baseURI, String queryString, List<NameValuePair> requestParams) {
		super(httpClient, baseURI, queryString);
		if (!requestParams.isEmpty() && !(httpClient instanceof ExtendedSPARQLProtocolSession)) {
			throw new UnsupportedOperationException(String.format("%s does not support HTTP request parameters", httpClient.getClass().getName()));
		}
		this.requestParams = requestParams;
	}

	@Override
	public boolean evaluate() throws QueryEvaluationException {

		SPARQLProtocolSession client = getHttpClient();
		if (client instanceof ExtendedSPARQLProtocolSession) {
			try {
				return ((ExtendedSPARQLProtocolSession)client).sendBooleanQuery(queryLanguage, getQueryString(), baseURI, dataset, getIncludeInferred(),
						getMaxExecutionTime(), requestParams, getBindingsArray());
			} catch (IOException | RepositoryException | MalformedQueryException e) {
				throw new QueryEvaluationException(e.getMessage(), e);
			}
		} else {
			return super.evaluate();
		}
	}

	private String getQueryString() {
		return QueryStringUtil.getBooleanQueryString(queryString, getBindings());
	}
}
