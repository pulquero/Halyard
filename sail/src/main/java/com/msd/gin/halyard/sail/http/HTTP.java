package com.msd.gin.halyard.sail.http;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public final class HTTP {
	private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

	public static final String NAMESPACE = "http://www.w3.org/2011/http#";

	private static IRI iri(String localName) {
		return SVF.createIRI(NAMESPACE, localName);
	}

	public static final IRI REQUEST_CLASS = iri("Request");
	public static final IRI METHOD_NAME_PROPERTY = iri("methodName");
	public static final IRI ABSOLUTE_PATH_PROPERTY = iri("absolutePath");
	public static final IRI ABSOLUTE_URI_PROPERTY = iri("absoluteURI");
	public static final IRI HEADERS_PROPERTY = iri("headers");
	public static final IRI REQUEST_HEADER_CLASS = iri("RequestHeader");
	public static final IRI FIELD_NAME_PROPERTY = iri("fieldName");
	public static final IRI FIELD_VALUE_PROPERTY = iri("fieldValue");
	public static final IRI RESP_PROPERTY = iri("resp");
	public static final IRI RESPONSE_CLASS = iri("Response");
	public static final IRI STATUS_CODE_VALUE_PROPERTY = iri("statusCodeValue");
	public static final IRI REASON_PHRASE_PROPERTY = iri("reasonPhrase");
	public static final IRI BODY_PROPERTY = iri("body");

	public static final class METHOD {
		public static final String NAMESPACE = "http://www.w3.org/2011/http-methods";
		private static final Map<String, IRI> IRI_MAP = new HashMap<>();

		private static IRI iri(String localName) {
			IRI iri = SVF.createIRI(NAMESPACE, localName);
			IRI_MAP.put(localName, iri);
			return iri;
		}

		public static final IRI CONNECT = iri("CONNECT");
		public static final IRI DELETE = iri("DELETE");
		public static final IRI GET = iri("GET");
		public static final IRI HEAD = iri("HEAD");
		public static final IRI OPTIONS = iri("OPTIONS");
		public static final IRI PATCH = iri("PATCH");
		public static final IRI POST = iri("POST");
		public static final IRI PUT = iri("PUT");
		public static final IRI TRACE = iri("TRACE");

		public static IRI toIRI(String methodName) {
			return IRI_MAP.get(methodName.toUpperCase());
		}
	}
}
