package com.msd.gin.halyard.client;

import java.io.IOException;
import java.util.List;

import org.apache.http.NameValuePair;
import org.eclipse.rdf4j.http.client.SPARQLProtocolSession;
import org.eclipse.rdf4j.http.protocol.UnauthorizedException;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLUpdate;

public class ExtendedSPARQLUpdate extends SPARQLUpdate {

	private final List<NameValuePair> requestParams;
	private String response;

	public ExtendedSPARQLUpdate(SPARQLProtocolSession httpClient, String baseURI, String queryString, List<NameValuePair> requestParams) {
		super(httpClient, baseURI, queryString);
		if (!requestParams.isEmpty() && !(httpClient instanceof ExtendedSPARQLProtocolSession)) {
			throw new UnsupportedOperationException(String.format("%s does not support HTTP request parameters", httpClient.getClass().getName()));
		}
		this.requestParams = requestParams;
	}

	@Override
	public void execute() throws UpdateExecutionException {
		SPARQLProtocolSession client = getHttpClient();
		if (client instanceof ExtendedSPARQLProtocolSession) {
			try {
				// execute update immediately
				try {
					response = ((ExtendedSPARQLProtocolSession)client).sendUpdate(getQueryLanguage(), getQueryString(), getBaseURI(), dataset, includeInferred,
							getMaxExecutionTime(), requestParams, getBindingsArray());
				} catch (UnauthorizedException | QueryInterruptedException | MalformedQueryException | IOException e) {
					throw new UpdateExecutionException(e.getMessage(), e);
				}
			} catch (RepositoryException e) {
				throw new UpdateExecutionException(e.getMessage(), e);
			}
		} else {
			super.execute();
		}

	}

	public String getResponse() {
		return response;
	}
}
