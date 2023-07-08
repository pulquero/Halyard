package com.msd.gin.halyard.common;

import javax.annotation.Nonnull;

import org.eclipse.rdf4j.model.Value;

public interface IdentifiableValue extends Value {
	ValueIdentifier getId(@Nonnull RDFFactory rdfFactory);
	ByteArray getSerializedForm(@Nonnull RDFFactory rdfFactory);
	void setId(@Nonnull RDFFactory rdfFactory, @Nonnull ValueIdentifier id);
	void setIdSer(@Nonnull RDFFactory rdfFactory, @Nonnull ValueIdentifier id, @Nonnull ByteArray ser);
}
