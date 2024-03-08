package com.msd.gin.halyard.model;

import com.msd.gin.halyard.model.MapLiteral;

import java.util.Collections;

import org.eclipse.rdf4j.model.Literal;

public class MapLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() throws Exception {
		return new MapLiteral(Collections.singletonMap("key", 5));
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		return new MapLiteral(Collections.singletonMap("foo", "bar"));
	}
}
