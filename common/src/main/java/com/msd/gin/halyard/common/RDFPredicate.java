package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;

public final class RDFPredicate extends RDFValue<IRI, SPOC.P> {
	static RDFPredicate create(@Nullable IRI pred, RDFFactory rdfFactory) {
		if(pred == null) {
			return null;
		}
		return new RDFPredicate(pred, rdfFactory);
	}

	private RDFPredicate(IRI val, RDFFactory rdfFactory) {
		super(RDFRole.Name.PREDICATE, val, rdfFactory);
	}
}
