package com.msd.gin.halyard.client;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.http.NameValuePair;
import org.eclipse.rdf4j.http.client.SPARQLProtocolSession;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

public class ExtendedSPARQLRepository extends SPARQLRepository {

	private final String graphStoreEndpointUrl;

	private boolean quadMode;

	private volatile List<NameValuePair> additionalHttpRequestParams = Collections.emptyList();

	public ExtendedSPARQLRepository(String endpointUrl) {
		this(endpointUrl, endpointUrl, endpointUrl);
	}

	public ExtendedSPARQLRepository(String queryEndpointUrl, String updateEndpointUrl, String graphStoreEndpointUrl) {
		super(queryEndpointUrl, updateEndpointUrl);
		this.graphStoreEndpointUrl = Objects.requireNonNull(graphStoreEndpointUrl);
		setHttpClientSessionManager(new ExtendedHttpClientSessionManager());
	}

	public List<NameValuePair> getAdditionalHttpRequestParameters() {
		return Collections.unmodifiableList(additionalHttpRequestParams);
	}

	public void setAdditionalHttpRequestParameters(List<NameValuePair> additionalHttpRequestParams) {
		if (additionalHttpRequestParams == null) {
			this.additionalHttpRequestParams = Collections.emptyList();
		} else {
			this.additionalHttpRequestParams = additionalHttpRequestParams;
		}
	}

	@Override
	public void enableQuadMode(boolean flag) {
		super.enableQuadMode(flag);
		this.quadMode = flag;
	}

	@Override
	protected SPARQLProtocolSession createSPARQLProtocolSession() {
		SPARQLProtocolSession session = super.createSPARQLProtocolSession();
		if (session instanceof ExtendedSPARQLProtocolSession) {
			ExtendedSPARQLProtocolSession extSession = (ExtendedSPARQLProtocolSession) session;
			extSession.setGraphStoreURL(graphStoreEndpointUrl);
			extSession.setAdditionalHttpRequestParameters(additionalHttpRequestParams);
		} else if (!additionalHttpRequestParams.isEmpty()) {
			throw new UnsupportedOperationException(String.format("%s does not support HTTP request parameters", session.getClass().getName()));
		}
		return session;
	}

	@Override
	public RepositoryConnection getConnection() throws RepositoryException {
		if (!isInitialized()) {
			init();
		}
		return new ExtendedSPARQLConnection(this, createSPARQLProtocolSession(), quadMode);
	}
}
