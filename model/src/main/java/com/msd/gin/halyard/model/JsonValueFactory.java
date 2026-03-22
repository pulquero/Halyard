package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;

public interface JsonValueFactory {
	Literal createJsonLiteral(String json);
}
