package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.TupleLiteral;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TupleLiteralFunctionTest {
	private final ValueFactory vf = SimpleValueFactory.getInstance();

	@Test
	public void test() {
		Value v1 = vf.createLiteral("foobar");
		Value v2 = vf.createIRI("http://foobar.org/");
		TupleLiteral l = (TupleLiteral) new TupleLiteralFunction().evaluate(vf, v1, v2);
		assertEquals(v1, l.objectValue()[0]);
		assertEquals(v2, l.objectValue()[1]);
	}
}
