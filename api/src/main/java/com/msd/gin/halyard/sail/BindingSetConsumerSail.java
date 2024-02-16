package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;

public interface BindingSetConsumerSail extends Sail {
	@Override
	BindingSetConsumerSailConnection getConnection() throws SailException;
}
