package com.msd.gin.halyard.common;

import com.msd.gin.halyard.model.TermRole;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Resource;

public final class RDFSubject extends RDFValue<Resource, SPOC.S> {
	static RDFSubject create(@Nullable Resource subj, RDFFactory rdfFactory, ValueIdentifier wellKnownIriId) {
		if(subj == null) {
			return null;
		}
		if (wellKnownIriId != null) {
			return new RDFSubject(subj, rdfFactory, wellKnownIriId);
		} else {
			return new RDFSubject(subj, rdfFactory);
		}
	}

	private RDFSubject(Resource val, RDFFactory rdfFactory) {
		super(TermRole.SUBJECT, val, rdfFactory);
	}

	private RDFSubject(Resource val, RDFFactory rdfFactory, ValueIdentifier wellKnownIriId) {
		super(TermRole.SUBJECT, val, rdfFactory, wellKnownIriId);
	}
}
