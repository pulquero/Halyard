package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.TermRole;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Value;

public final class RDFObject extends RDFValue<Value, SPOC.O> {
	static RDFObject create(@Nullable Value obj, RDFFactory rdfFactory, ValueIdentifier wellKnownIriId) {
		if(obj == null) {
			return null;
		}
		if (wellKnownIriId != null) {
			return new RDFObject(obj, rdfFactory, wellKnownIriId);
		} else {
			return new RDFObject(obj, rdfFactory);
		}
	}

	private RDFObject(Value val, RDFFactory rdfFactory) {
		super(TermRole.OBJECT, val, rdfFactory);
	}

	private RDFObject(Value val, RDFFactory rdfFactory, ValueIdentifier wellKnownIriId) {
		super(TermRole.OBJECT, val, rdfFactory, wellKnownIriId);
	}
}
