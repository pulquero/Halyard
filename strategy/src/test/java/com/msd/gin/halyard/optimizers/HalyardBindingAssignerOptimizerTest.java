package com.msd.gin.halyard.optimizers;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HalyardBindingAssignerOptimizerTest {
	public QueryOptimizer getOptimizer() {
		return new HalyardBindingAssignerOptimizer();
	}

	@Test
	public void testValues() {
		String expectedQuery = "SELECT * WHERE {BIND(3 as ?x) BIND('c' as ?y) VALUES (?x ?y) {(1 'a') (2 'b') (3 'c') (4 'd')}}";
		String query = "SELECT * WHERE {VALUES (?x ?y) {(1 'a') (2 'b') (3 'c') (4 'd')}}";
		QueryBindingSet bindings = new QueryBindingSet();
		bindings.setBinding("x", SimpleValueFactory.getInstance().createLiteral("3", XSD.INTEGER));
		bindings.setBinding("y", SimpleValueFactory.getInstance().createLiteral("c"));
		testOptimizer(expectedQuery, query, bindings);
	}

	void testOptimizer(String expectedQuery, String actualQuery, BindingSet bindings) {
		ParsedQuery pq = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, actualQuery, null);
		QueryOptimizer opt = getOptimizer();
		opt.optimize(pq.getTupleExpr(), null, bindings);

		ParsedQuery expectedParsedQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, expectedQuery, null);
		assertEquals(expectedParsedQuery.getTupleExpr(), pq.getTupleExpr());
	}
}
