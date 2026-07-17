package com.msd.gin.halyard.model.vocabulary;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;

public enum HalyardDatatype implements CoreDatatype {
	ARRAY(HALYARD.ARRAY_TYPE),
	MAP(HALYARD.MAP_TYPE),
	TUPLE(HALYARD.TUPLE_TYPE);

	private final IRI iri;

	HalyardDatatype(IRI iri) {
		this.iri = iri;
	}

	@Override
	public IRI getIri() {
		return iri;
	}

	private final static Map<IRI,CoreDatatype> lookup;

	static {
		HashMap<IRI,CoreDatatype> map = new HashMap<>();
		for (HalyardDatatype dt : HalyardDatatype.values()) {
			map.put(dt.getIri(), dt);
		}
		lookup = Collections.unmodifiableMap(map);
	}

	public static CoreDatatype from(IRI datatype) {
		return lookup.getOrDefault(datatype, CoreDatatype.NONE);
	}
}
