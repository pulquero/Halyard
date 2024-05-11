/**
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.msd.gin.halyard.sail.search;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.msd.gin.halyard.common.IdentifiableValue;
import com.msd.gin.halyard.common.RDFFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

public class SearchDocument {
	public static final String ID_FIELD = "id";
	public static final String IRI_FIELD = "iri";
	public static final String LABEL_FIELD = "label";
	public static final String NUMBER_SUBFIELD = "number";
	public static final String INTEGER_SUBFIELD = "integer";
	public static final String POINT_SUBFIELD = "point";
	public static final String LABEL_NUMBER_FIELD = LABEL_FIELD + "." + NUMBER_SUBFIELD;
	public static final String LABEL_INTEGER_FIELD = LABEL_FIELD + "." + INTEGER_SUBFIELD;
	public static final String LABEL_POINT_FIELD = LABEL_FIELD + "." + POINT_SUBFIELD;
	public static final String LANG_FIELD = "lang";
	public static final String DATATYPE_FIELD = "datatype";
	public static final String GEOMETRY_FIELD = "geometry";
	static final List<String> REQUIRED_FIELDS = Arrays.asList(ID_FIELD, IRI_FIELD, LABEL_FIELD, LANG_FIELD, DATATYPE_FIELD);

	@JsonProperty(ID_FIELD)
	public String id;
	@JsonProperty(IRI_FIELD)
	public String iri;
	@JsonProperty(LABEL_FIELD)
	public String label;
	@JsonProperty(LANG_FIELD)
	public String lang;
	@JsonProperty(DATATYPE_FIELD)
	public String datatype;
	@JsonProperty(GEOMETRY_FIELD)
	public String geometry;
	@JsonIgnore
	private Map<String, Object> additionalFields;

	public Object getAdditionalField(String name) {
		return (additionalFields != null) ? additionalFields.get(name) : null;
	}

	@JsonAnySetter
	public void setAdditionalField(String name, Object value) {
		if (additionalFields == null) {
			additionalFields = new HashMap<>();
		}
		additionalFields.put(name, value);
	}

	@Override
	public String toString() {
		return String.format("id: %s, iri: %s, label: %s, datatype: %s, lang: %s, geometry: %s", id, iri, label, datatype, lang, geometry);
	}

	public Value createValue(ValueFactory vf, RDFFactory rdfFactory) {
		Value v;
		if (iri != null) {
			v = vf.createIRI(iri);
		} else if (lang != null) {
			v = vf.createLiteral(label, lang);
		} else {
			v = vf.createLiteral(label, vf.createIRI(datatype));
		}
		if (v instanceof IdentifiableValue) {
			((IdentifiableValue) v).setId(rdfFactory.idFromString(id), rdfFactory);
		}
		return v;
	}
}