package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;

public interface BindingSetCallbackSail extends Sail {
	@Override
	BindingSetCallbackSailConnection getConnection() throws SailException;
}
