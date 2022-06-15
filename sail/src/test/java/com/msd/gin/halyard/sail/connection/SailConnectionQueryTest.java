package com.msd.gin.halyard.sail.connection;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SailConnectionQueryTest {
	private Sail sail;
	private SailConnection conn;

	@Before
	public void setup() {
		sail = new MemoryStore();
		conn = sail.getConnection();
		conn.begin();
		conn.addStatement(sail.getValueFactory().createBNode(), RDF.TYPE, RDF.LIST);
		conn.commit();
	}

	@Test
	public void testBooleanQuery() {
		ParsedBooleanQuery q = (ParsedBooleanQuery) QueryParserUtil.parseQuery(QueryLanguage.SPARQL, "ask {?s ?p ?o}", null);
		SailConnectionBooleanQuery sq = new SailConnectionBooleanQuery(q, conn);
		assertTrue(sq.evaluate());
	}

	@Test
	public void testTupleQuery() {
		ParsedTupleQuery q = (ParsedTupleQuery) QueryParserUtil.parseQuery(QueryLanguage.SPARQL, "select * {?s ?p ?o}", null);
		SailConnectionTupleQuery sq = new SailConnectionTupleQuery(q, conn);
		assertTrue(sq.evaluate().hasNext());
	}

	@Test
	public void testGraphQuery() {
		ParsedGraphQuery q = (ParsedGraphQuery) QueryParserUtil.parseQuery(QueryLanguage.SPARQL, "construct {?s ?p ?o} where {?s ?p ?o}", null);
		SailConnectionGraphQuery sq = new SailConnectionGraphQuery(q, conn, sail.getValueFactory());
		assertTrue(sq.evaluate().hasNext());
	}
}
