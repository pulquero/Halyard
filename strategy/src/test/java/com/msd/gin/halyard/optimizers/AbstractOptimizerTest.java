package com.msd.gin.halyard.optimizers;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractOptimizerTest {
	protected abstract QueryOptimizer getOptimizer();

	protected final void testOptimizer(String expectedQuery, String actualQuery) {
		testOptimizer(expectedQuery, actualQuery, null);
	}

	protected final void testOptimizer(String expectedQuery, String actualQuery, BindingSet bindings) {
		ParsedQuery pq = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, actualQuery, null);
		QueryOptimizer opt = getOptimizer();
		opt.optimize(pq.getTupleExpr(), pq.getDataset(), bindings);

		ParsedQuery expectedParsedQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, expectedQuery, null);
		assertEquals(expectedParsedQuery.getTupleExpr(), pq.getTupleExpr());
	}

	protected final void testOptimizer(TupleExpr expectedQuery, String actualQuery) {
		ParsedQuery pq = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, actualQuery, null);
		QueryOptimizer opt = getOptimizer();
		opt.optimize(pq.getTupleExpr(), pq.getDataset(), EmptyBindingSet.getInstance());

		assertEquals(expectedQuery, pq.getTupleExpr());
	}
}
