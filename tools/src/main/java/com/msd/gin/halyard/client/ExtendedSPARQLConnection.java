package com.msd.gin.halyard.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.http.NameValuePair;
import org.eclipse.rdf4j.http.client.SPARQLProtocolSession;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.UnsupportedQueryLanguageException;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.SPARQLConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;

import static org.eclipse.rdf4j.query.QueryLanguage.*;

public class ExtendedSPARQLConnection extends SPARQLConnection {

	private final SPARQLProtocolSession client;

	private List<NameValuePair> requestParams = Collections.emptyList();

	public ExtendedSPARQLConnection(SPARQLRepository repository, SPARQLProtocolSession client, boolean quadMode) {
		super(repository, client, quadMode);
		this.client = client;
	}

	public List<NameValuePair> getAdditionalHttpRequestParameters() {
		return Collections.unmodifiableList(requestParams);
	}

	public void setAdditionalHttpRequestParameters(List<NameValuePair> additionalHttpRequestParams) {
		if (additionalHttpRequestParams == null) {
			this.requestParams = Collections.emptyList();
		} else {
			this.requestParams = additionalHttpRequestParams;
		}
	}

	@Override
	public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query, String base)
			throws RepositoryException, MalformedQueryException {
		if (SPARQL.equals(ql)) {
			return new ExtendedSPARQLBooleanQuery(client, base, query, requestParams);
		}
		throw new UnsupportedQueryLanguageException("Unsupported query language " + ql);
	}

	@Override
	public GraphQuery prepareGraphQuery(QueryLanguage ql, String query, String base)
			throws RepositoryException, MalformedQueryException {
		if (SPARQL.equals(ql)) {
			return new ExtendedSPARQLGraphQuery(client, base, query, requestParams);
		}
		throw new UnsupportedQueryLanguageException("Unsupported query language " + ql);
	}

	@Override
	public TupleQuery prepareTupleQuery(QueryLanguage ql, String query, String base)
			throws RepositoryException, MalformedQueryException {
		if (SPARQL.equals(ql)) {
			return new ExtendedSPARQLTupleQuery(client, base, query, requestParams);
		}
		throw new UnsupportedQueryLanguageException("Unsupported query language " + ql);
	}

	@Override
	public Update prepareUpdate(QueryLanguage ql, String update, String baseURI)
			throws RepositoryException, MalformedQueryException {
		if (SPARQL.equals(ql)) {
			return new ExtendedSPARQLUpdate(client, baseURI, update, requestParams);
		}
		throw new UnsupportedQueryLanguageException("Unsupported query language " + ql);
	}

	@Override
	public void add(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts)
			throws IOException, RDFParseException, RepositoryException {
		if (client instanceof ExtendedSPARQLProtocolSession) {
			Resource[] graphs = contexts.length > 0 ? contexts : new Resource[] {null};
			for (Resource graph : graphs) {
				((ExtendedSPARQLProtocolSession)client).storeGraph(in, dataFormat, graph, requestParams);
			}
		} else {
			super.add(in, baseURI, dataFormat, contexts);
		}
	}

	@Override
	public void add(Reader reader, String baseURI, RDFFormat dataFormat, Resource... contexts)
			throws IOException, RDFParseException, RepositoryException {
		if (client instanceof ExtendedSPARQLProtocolSession) {
			Resource[] graphs = contexts.length > 0 ? contexts : new Resource[] {null};
			for (Resource graph : graphs) {
				((ExtendedSPARQLProtocolSession)client).storeGraph(new ReaderInputStream(reader, StandardCharsets.UTF_8), dataFormat, graph, requestParams);
			}
		} else {
			super.add(reader, baseURI, dataFormat, contexts);
		}
	}

	@Override
	public void add(File file, String baseURI, RDFFormat dataFormat, Resource... contexts)
			throws IOException, RDFParseException, RepositoryException {
		if (client instanceof ExtendedSPARQLProtocolSession) {
			Resource[] graphs = contexts.length > 0 ? contexts : new Resource[] {null};
			for (Resource graph : graphs) {
				((ExtendedSPARQLProtocolSession)client).storeGraph(file, dataFormat, graph, requestParams);
			}
		} else {
			super.add(file, baseURI, dataFormat, contexts);
		}
	}

	@Override
	public void clear(Resource... contexts) throws RepositoryException {
		if (client instanceof ExtendedSPARQLProtocolSession) {
			try {
				if (contexts.length > 0) {
					for (Resource graph : contexts) {
						((ExtendedSPARQLProtocolSession)client).deleteGraph(graph, requestParams);
					}
				} else {
					((ExtendedSPARQLProtocolSession)client).deleteAll(requestParams);
				}
			} catch (IOException ioe) {
				throw new RepositoryException(ioe);
			}
		} else {
			super.clear(contexts);
		}
	}
}
