package com.msd.gin.halyard.rio;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.helpers.ContextStatementCollector;
import org.junit.jupiter.api.Test;

public class TriGStarExtendedTest {
	@Test
	public void testAnnotations() throws IOException {
		Set<Statement> stmts = new HashSet<>();
		TriGStarParser parser = new TriGStarParser();
		ValueFactory vf = SimpleValueFactory.getInstance();
		parser.setRDFHandler(new ContextStatementCollector(stmts, vf));
		parser.parse(new StringReader("<x:foobar> <x:name> \"foobar\" {| <x:source> <https://somewhere> |}."));
		assertThat(stmts).containsExactly(
				vf.createStatement(vf.createIRI("x:foobar"), vf.createIRI("x:name"), vf.createLiteral("foobar")),
				vf.createStatement(vf.createTriple(vf.createIRI("x:foobar"), vf.createIRI("x:name"), vf.createLiteral("foobar")), vf.createIRI("x:source"), vf.createIRI("https://somewhere"))
		);
	}
}
