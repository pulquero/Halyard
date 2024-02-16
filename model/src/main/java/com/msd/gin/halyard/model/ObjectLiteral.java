package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Literal;

public interface ObjectLiteral<T> extends Literal {
	T objectValue();
}
