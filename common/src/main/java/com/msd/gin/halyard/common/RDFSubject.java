package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Resource;

public final class RDFSubject extends RDFValue<Resource, SPOC.S> {
	static RDFSubject create(@Nullable Resource subj, RDFFactory rdfFactory) {
		if(subj == null) {
			return null;
		}
		return new RDFSubject(subj, rdfFactory);
	}

	private RDFSubject(Resource val, RDFFactory rdfFactory) {
		super(RDFRole.Name.SUBJECT, val, rdfFactory);
	}
}
