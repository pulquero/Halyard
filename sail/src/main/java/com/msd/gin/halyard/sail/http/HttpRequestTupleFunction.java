package com.msd.gin.halyard.sail.http;

import com.google.common.base.Strings;
import com.msd.gin.halyard.model.Base64Literal;
import com.msd.gin.halyard.model.MapLiteral;
import com.msd.gin.halyard.model.ObjectArrayLiteral;
import com.msd.gin.halyard.model.XMLLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.function.ExtendedTupleFunction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype.XSD;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;

@MetaInfServices(TupleFunction.class)
public class HttpRequestTupleFunction implements ExtendedTupleFunction {

	@Override
	public String getURI() {
		return HALYARD.HTTP_REQUEST_FUNCTION.stringValue();
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>> evaluate(TripleSource tripleSource, Value... args) throws QueryEvaluationException {
		ExtendedTripleSource extTripleSource = (ExtendedTripleSource) tripleSource;
		ValueFactory vf = extTripleSource.getValueFactory();
		if (args.length < 1) {
			throw new QueryEvaluationException("Missing arguments");
		}

		if (!args[0].isLiteral()) {
			throw new QueryEvaluationException("Invalid URI value");
		}

		String uri = ((Literal) args[0]).getLabel();
		IRI method;
		if (args.length > 1) {
			if (args[1].isLiteral()) {
				method = HTTP.METHOD.toIRI(((Literal) args[1]).getLabel());
			} else if (args[1].isIRI()) {
				method = (IRI) args[1];
			} else {
				throw new QueryEvaluationException("Invalid HTTP method value");
			}
		} else {
			method = HTTP.METHOD.GET;
		}
		Object[] headers;
		if (args.length > 2) {
			headers = ObjectArrayLiteral.objectArray((Literal) args[2]);
		} else {
			headers = new Object[0];
		}
		Literal requestBody;
		if (args.length > 3 && !RDF.NIL.equals(args[3])) {
			requestBody = (Literal) args[3];
		} else {
			requestBody = null;
		}

		RequestBuilder requestBuilder = RequestBuilder.create(method.getLocalName()).setUri(uri);
		for (Object headerObj : headers) {
			Header header = toHeader(headerObj);
			requestBuilder.addHeader(header);
		}
		if (requestBody != null) {
			HttpEntity entity;
			if (XSD.BASE64BINARY.equals(requestBody.getCoreDatatype())) {
				entity = new ByteArrayEntity(Base64Literal.byteArray(requestBody));
			} else {
				ByteBuffer bb = StandardCharsets.UTF_8.encode(requestBody.stringValue());
				byte[] content = new byte[bb.remaining()];
				bb.get(content);
				entity = new ByteArrayEntity(content);
			}
			requestBuilder.setEntity(entity);
		}
		HttpUriRequest request = requestBuilder.build();
		try (final CloseableHttpClient httpClient = HttpClients.createDefault()) {
			try (final CloseableHttpResponse resp = httpClient.execute(request)) {
				StatusLine sl = resp.getStatusLine();
				int sc = sl.getStatusCode();
				String reason = sl.getReasonPhrase();
				Literal scLiteral = vf.createLiteral(sc);
				Literal reasonLiteral = vf.createLiteral(Strings.nullToEmpty(reason));
				HttpEntity entity = resp.getEntity();
				ContentType contentType = ContentType.get(entity);
				if (contentType == null) {
					contentType = ContentType.APPLICATION_OCTET_STREAM;
				}
				String mimeType = contentType.getMimeType();
				Literal respLiteral;
				if (ContentType.APPLICATION_JSON.getMimeType().equals(mimeType)) {
					respLiteral = new MapLiteral(EntityUtils.toString(entity));
				} else if (mimeType.endsWith("+xml") || ContentType.APPLICATION_XML.getMimeType().equals(mimeType) || ContentType.TEXT_XML.getMimeType().equals(mimeType)) {
					respLiteral = new XMLLiteral(EntityUtils.toString(entity));
				} else if (mimeType.startsWith("text/")) {
					respLiteral = vf.createLiteral(EntityUtils.toString(entity));
				} else {
					respLiteral = new Base64Literal(EntityUtils.toByteArray(entity));
				}
				return new SingletonIteration<List<? extends Value>>(Arrays.asList(scLiteral, reasonLiteral, respLiteral));
			}
		} catch (IOException ioe) {
			throw new QueryEvaluationException(ioe);
		}
	}

	private static Header toHeader(Object o) {
		if (o instanceof Pair<?, ?>) {
			Pair<String, String> kv = (Pair<String, String>) o;
			return new BasicHeader(kv.getKey(), kv.getValue());
		} else if (o instanceof Map<?, ?>) {
			Map<String, String> json = (Map<String, String>) o;
			return new BasicHeader(json.get("fieldName"), json.get("fieldValue"));
		} else {
			throw new QueryEvaluationException("Invalid HTTP header value");
		}
	}
}
