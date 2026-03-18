package com.msd.gin.halyard.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;

import org.eclipse.rdf4j.model.Literal;
import org.junit.jupiter.api.Test;

public class MapLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() throws Exception {
		return new MapLiteral(Collections.singletonMap("key", 5));
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		return new MapLiteral(Collections.singletonMap("foo", "bar"));
	}

	@Test
	public void testParse() {
		MapLiteral l = new MapLiteral("{\"foo\":\"bar\"}");
		assertEquals("bar", l.objectValue().get("foo"));
	}
}
