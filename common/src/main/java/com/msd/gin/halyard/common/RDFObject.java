package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.TermRole;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Value;

public final class RDFObject extends RDFValue<Value, SPOC.O> {
	static RDFObject create(@Nullable Value obj, RDFFactory rdfFactory) {
		if(obj == null) {
			return null;
		}
		return new RDFObject(obj, rdfFactory);
	}

	private RDFObject(Value val, RDFFactory rdfFactory) {
		super(TermRole.OBJECT, val, rdfFactory);
	}
}
