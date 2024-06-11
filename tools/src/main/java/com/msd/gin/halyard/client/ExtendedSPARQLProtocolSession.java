package com.msd.gin.halyard.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.http.client.SPARQLProtocolSession;
import org.eclipse.rdf4j.http.protocol.Protocol;
import org.eclipse.rdf4j.http.protocol.UnauthorizedException;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;

public class ExtendedSPARQLProtocolSession extends SPARQLProtocolSession {

	private String graphStoreUrl;
	private List<NameValuePair> additionalHttpRequestParams = Collections.emptyList();

	public ExtendedSPARQLProtocolSession(HttpClient client, ExecutorService executor) {
		super(client, executor);
	}

	@Override
	public void setQueryURL(String url) {
		super.setQueryURL(url);
	}

	@Override
	public void setUpdateURL(String url) {
		super.setUpdateURL(url);
	}

	public void setGraphStoreURL(String url) {
		this.graphStoreUrl = url;
	}

	public String getGraphStoreURL() {
		return graphStoreUrl;
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

	public boolean sendBooleanQuery(QueryLanguage ql, String query, String baseURI, Dataset dataset,
			boolean includeInferred, int maxQueryTime, List<NameValuePair> requestParams, Binding... bindings) throws IOException, RepositoryException,
			MalformedQueryException, UnauthorizedException, QueryInterruptedException {
		HttpUriRequest method = getQueryMethod(ql, query, baseURI, dataset, includeInferred, maxQueryTime, requestParams, bindings);
		try {
			return getBoolean(method);
		} catch (RepositoryException | MalformedQueryException | QueryInterruptedException e) {
			throw e;
		} catch (RDF4JException e) {
			throw new RepositoryException(e);
		}
	}

	public TupleQueryResult sendTupleQuery(QueryLanguage ql, String query, String baseURI, Dataset dataset,
			boolean includeInferred, int maxQueryTime, List<NameValuePair> requestParams, Binding... bindings)
			throws IOException, RepositoryException,
			MalformedQueryException, UnauthorizedException, QueryInterruptedException {
		HttpUriRequest method = getQueryMethod(ql, query, baseURI, dataset, includeInferred, maxQueryTime, requestParams, bindings);
		return getBackgroundTupleQueryResult(method, null);
	}

	public void sendTupleQuery(QueryLanguage ql, String query, String baseURI, Dataset dataset, boolean includeInferred,
			int maxQueryTime, TupleQueryResultHandler handler, List<NameValuePair> requestParams, Binding... bindings)
			throws IOException, TupleQueryResultHandlerException, RepositoryException, MalformedQueryException,
			UnauthorizedException, QueryInterruptedException {
		HttpUriRequest method = getQueryMethod(ql, query, baseURI, dataset, includeInferred, maxQueryTime, requestParams, bindings);
		getTupleQueryResult(method, handler);
	}

	public void sendGraphQuery(QueryLanguage ql, String query, String baseURI, Dataset dataset, boolean includeInferred,
			int maxQueryTime, RDFHandler handler, List<NameValuePair> requestParams, Binding... bindings) throws IOException, RDFHandlerException,
			RepositoryException, MalformedQueryException, UnauthorizedException, QueryInterruptedException {
		HttpUriRequest method = getQueryMethod(ql, query, baseURI, dataset, includeInferred, maxQueryTime, requestParams, bindings);
		getRDF(method, handler, false);
	}

	public GraphQueryResult sendGraphQuery(QueryLanguage ql, String query, String baseURI, Dataset dataset,
			boolean includeInferred, int maxQueryTime, List<NameValuePair> requestParams, Binding... bindings)
			throws IOException, RepositoryException,
			MalformedQueryException, UnauthorizedException, QueryInterruptedException {
		try {
			HttpUriRequest method = getQueryMethod(ql, query, baseURI, dataset, includeInferred, maxQueryTime,
					requestParams, bindings);
			return getRDFBackground(method, false, null);
		} catch (RDFHandlerException e) {
			// Found a bug in TupleQueryResultBuilder?
			throw new RepositoryException(e);
		}
	}

	public String sendUpdate(QueryLanguage ql, String update, String baseURI, Dataset dataset, boolean includeInferred,
			int maxQueryTime, List<NameValuePair> requestParams, Binding... bindings) throws IOException, RepositoryException, MalformedQueryException,
			UnauthorizedException, QueryInterruptedException {
		HttpUriRequest method = getUpdateMethod(ql, update, baseURI, dataset, includeInferred, maxQueryTime, requestParams, bindings);

		HttpResponse response = execute(method);
		try {
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode >= 300) {
				throw new RepositoryException("Request failed with status " + httpCode + ": "
						+ method.getURI().toString());
			}
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				return EntityUtils.toString(entity, StandardCharsets.UTF_8);
			} else {
				return null;
			}
		} catch (RepositoryException | MalformedQueryException | QueryInterruptedException e) {
			throw e;
		} catch (RDF4JException e) {
			throw new RepositoryException(e);
		} finally {
			EntityUtils.consume(response.getEntity());
		}
	}

	protected HttpUriRequest getQueryMethod(QueryLanguage ql, String query, String baseURI, Dataset dataset,
			boolean includeInferred, int maxQueryTime, List<NameValuePair> requestParams, Binding... bindings) {
		List<NameValuePair> queryParams = getQueryMethodParameters(ql, query, baseURI, dataset, includeInferred,
				maxQueryTime, bindings);
		queryParams.addAll(requestParams);
		HttpUriRequest method;
		String queryUrlWithParams = createURL(getQueryURL(), queryParams);
		if (shouldUsePost(queryUrlWithParams)) {
			// we just built up a URL for nothing. oh well.
			// It's probably not much overhead against
			// the poor triplestore having to process such as massive query
			HttpPost postMethod = new HttpPost(getQueryURL());
			postMethod.setHeader("Content-Type", Protocol.FORM_MIME_TYPE + "; charset=utf-8");
			postMethod.setEntity(new UrlEncodedFormEntity(queryParams, UTF8));
			method = postMethod;
		} else {
			method = new HttpGet(queryUrlWithParams);
		}
		// functionality to provide custom http headers as required by the
		// applications
		for (Map.Entry<String, String> additionalHeader : getAdditionalHttpHeaders().entrySet()) {
			method.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
		}
		return method;
	}

	protected HttpUriRequest getUpdateMethod(QueryLanguage ql, String update, String baseURI, Dataset dataset,
			boolean includeInferred, int maxQueryTime, List<NameValuePair> requestParams, Binding... bindings) {
		HttpPost method = new HttpPost(getUpdateURL());

		method.setHeader("Content-Type", Protocol.FORM_MIME_TYPE + "; charset=utf-8");

		List<NameValuePair> queryParams = getUpdateMethodParameters(ql, update, baseURI, dataset, includeInferred,
				maxQueryTime, bindings);

		method.setEntity(new UrlEncodedFormEntity(queryParams, UTF8));

		for (Map.Entry<String, String> additionalHeader : getAdditionalHttpHeaders().entrySet()) {
			method.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
		}

		return method;
	}

	@Override
	protected List<NameValuePair> getQueryMethodParameters(QueryLanguage ql, String query, String baseURI,
			Dataset dataset, boolean includeInferred, int maxQueryTime, Binding... bindings) {
		List<NameValuePair> queryParams = super.getQueryMethodParameters(ql, query, baseURI, dataset, includeInferred, maxQueryTime, bindings);
		queryParams.addAll(additionalHttpRequestParams);
		return queryParams;
	}

	@Override
	protected List<NameValuePair> getUpdateMethodParameters(QueryLanguage ql, String update, String baseURI,
			Dataset dataset, boolean includeInferred, int maxQueryTime, Binding... bindings) {
		List<NameValuePair> queryParams = super.getUpdateMethodParameters(ql, update, baseURI, dataset, includeInferred, maxQueryTime, bindings);
		queryParams.addAll(additionalHttpRequestParams);
		return queryParams;
	}

	public void storeGraph(InputStream in, RDFFormat dataFormat, Resource graph, List<NameValuePair> requestParams) throws IOException {
		storeGraph(new InputStreamEntity(in), dataFormat, graph, requestParams);
	}

	public void storeGraph(File file, RDFFormat dataFormat, Resource graph, List<NameValuePair> requestParams) throws IOException {
		if (dataFormat == null) {
			dataFormat = Rio.getParserFormatForFileName(file.getName())
					.orElseThrow(() -> new UnsupportedRDFormatException(
							"Could not find RDF format for file: " + file.getName()));
		}
		storeGraph(new FileEntity(file), dataFormat, graph, requestParams);
	}

	protected void storeGraph(HttpEntity entity, RDFFormat dataFormat, Resource graph, List<NameValuePair> requestParams) throws IOException {
		List<NameValuePair> queryParams = new ArrayList<>();
		if (graph == null) {
			queryParams.add(new BasicNameValuePair("default", null));
		} else {
			queryParams.add(new BasicNameValuePair("graph", graph.stringValue()));
		}
		queryParams.addAll(additionalHttpRequestParams);
		queryParams.addAll(requestParams);

		HttpPost method = new HttpPost(createURL(getGraphStoreURL(), queryParams));
		method.setHeader("Content-Type", dataFormat.getDefaultMIMEType());
		method.setEntity(entity);
		for (Map.Entry<String, String> additionalHeader : getAdditionalHttpHeaders().entrySet()) {
			method.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
		}

		executeNoContent(method);
	}

	public void deleteGraph(Resource graph, List<NameValuePair> requestParams) throws IOException {
		List<NameValuePair> queryParams = new ArrayList<>();
		if (graph == null) {
			queryParams.add(new BasicNameValuePair("default", null));
		} else {
			queryParams.add(new BasicNameValuePair("graph", graph.stringValue()));
		}
		queryParams.addAll(additionalHttpRequestParams);
		queryParams.addAll(requestParams);

		HttpDelete method = new HttpDelete(createURL(getGraphStoreURL(), queryParams));
		for (Map.Entry<String, String> additionalHeader : getAdditionalHttpHeaders().entrySet()) {
			method.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
		}

		executeNoContent(method);
	}

	public void deleteAll(List<NameValuePair> requestParams) throws IOException {
		List<NameValuePair> queryParams = new ArrayList<>();
		queryParams.addAll(additionalHttpRequestParams);
		queryParams.addAll(requestParams);

		HttpDelete method = new HttpDelete(createURL(getGraphStoreURL(), queryParams));
		for (Map.Entry<String, String> additionalHeader : getAdditionalHttpHeaders().entrySet()) {
			method.addHeader(additionalHeader.getKey(), additionalHeader.getValue());
		}

		executeNoContent(method);
	}

	private static String createURL(String endpoint, List<NameValuePair> queryParams) {
		try {
			URIBuilder urib = new URIBuilder(endpoint);
			for (NameValuePair nvp : queryParams) {
				urib.addParameter(nvp.getName(), nvp.getValue());
			}
			return urib.toString();
		} catch (URISyntaxException e) {
			throw new AssertionError(e);
		}
	}
}
