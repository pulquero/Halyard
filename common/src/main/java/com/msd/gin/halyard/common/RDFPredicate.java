package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.TermRole;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;

public final class RDFPredicate extends RDFValue<IRI, SPOC.P> {
	static RDFPredicate create(@Nullable IRI pred, RDFFactory rdfFactory, ValueIdentifier wellKnownIriId) {
		if(pred == null) {
			return null;
		}
		if (wellKnownIriId != null) {
			return new RDFPredicate(pred, rdfFactory, wellKnownIriId);
		} else {
			return new RDFPredicate(pred, rdfFactory);
		}
	}

	private RDFPredicate(IRI val, RDFFactory rdfFactory) {
		super(TermRole.PREDICATE, val, rdfFactory);
	}

	private RDFPredicate(IRI val, RDFFactory rdfFactory, ValueIdentifier wellKnownIriId) {
		super(TermRole.PREDICATE, val, rdfFactory, wellKnownIriId);
	}
}
