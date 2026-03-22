package com.msd.gin.halyard.spin.function.halyard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

import com.msd.gin.halyard.model.JsonOrgJsonValueFactory;
import com.msd.gin.halyard.model.JsonValueFactory;

public class JsonPathTupleFunctionTest {
	@Test
	public void testSimplePath() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		JsonValueFactory jsonvf = new JsonOrgJsonValueFactory(vf);
		String json = "{\"foo\":\"bar\"}";
		Literal l = jsonvf.createJsonLiteral(json);
		String jsonPath = "$.foo";
		CloseableIteration<? extends List<? extends Value>> iter = new JsonPathTupleFunction().evaluate(vf, vf.createLiteral(jsonPath), l);
		assertTrue(iter.hasNext());
		assertEquals("[\"bar\"]", iter.next().get(0).stringValue());
		assertFalse(iter.hasNext());
	}
}
