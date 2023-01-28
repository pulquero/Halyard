package com.msd.gin.halyard.common;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.Resource;

public final class RDFContext extends RDFValue<Resource, SPOC.C> {
	static RDFContext create(@Nullable Resource ctx, RDFFactory rdfFactory) {
		if(ctx == null) {
			return null;
		}
		if (ctx.isTriple()) {
    		throw new UnsupportedOperationException("Context cannot be a triple value");
		}
		return new RDFContext(ctx, rdfFactory);
	}

	private RDFContext(Resource val, RDFFactory rdfFactory) {
		super(RDFRole.Name.CONTEXT, val, rdfFactory);
	}
}
