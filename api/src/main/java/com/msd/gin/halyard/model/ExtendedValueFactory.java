package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;

public interface ExtendedValueFactory extends ValueFactory {
	/**
	 * Creates a literal from another literal implementation.
	 */
	default Literal createLiteral(Literal l) {
		return l;
	}
}
