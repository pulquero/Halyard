/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.strategy;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.model.vocabulary.SCHEMA_ORG;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResult;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Adam Sotona (MSD)
 */
public class HalyardStrategyExtendedTest {

    private Repository repo;
    private RepositoryConnection con;

    @Before
    public void setUp() throws Exception {
        repo = new SailRepository(new MockSailWithHalyardStrategy());
        repo.init();
        con = repo.getConnection();
    }

    @After
    public void tearDown() throws Exception {
        con.close();
        repo.shutDown();
    }

    @Test
    public void testService() {
        String sparql = "SELECT * WHERE {SERVICE <repository:memory> { VALUES ?s {1} }}";
        try (TupleQueryResult res = con.prepareTupleQuery(sparql).evaluate()) {
            res.hasNext();
	        assertEquals(1, ((Literal) res.next().getValue("s")).intValue());
        }
    }

    @Test(expected = QueryEvaluationException.class)
    public void testServiceNotReachable() throws Exception {
        String sparql = "SELECT * WHERE {SERVICE <http://whatever/> { ?s ?p ?o . }}";
        try (TupleQueryResult res = con.prepareTupleQuery(sparql).evaluate()) {
            res.hasNext();
        }
    }

    @Test
    public void testServiceSilent() throws Exception {
        String sparql = "SELECT * WHERE {SERVICE SILENT <http://whatever/> { ?s ?p ?o . }}";
        try (TupleQueryResult res = con.prepareTupleQuery(sparql).evaluate()) {
            res.hasNext();
        }
    }

    @Test
    public void testServicePushOnly() {
        String sparql = "SELECT * WHERE {SERVICE <repository:pushOnly> { VALUES ?s {1} }}";
        try (TupleQueryResult res = con.prepareTupleQuery(sparql).evaluate()) {
            res.hasNext();
	        assertEquals(1, ((Literal) res.next().getValue("s")).intValue());
        }
    }

    @Test
    public void testAskSailFederatedService() {
    	// SERVICE query with all bound variables should be evaluated as an ASK query
        String sparql = "SELECT * WHERE {VALUES (?s ?p ?o) {(<http://whatever> <http://whatever> <http://whatever>)} SERVICE <repository:askOnly> { ?s ?p ?o }}";
        try (TupleQueryResult res = con.prepareTupleQuery(sparql).evaluate()) {
            res.hasNext();
        }
    }

    @Test
    public void testReduced() throws Exception {
        String sparql = "SELECT REDUCED ?a WHERE {VALUES ?a {0 0 1 1 0 0 1 1}}";
        try (TupleQueryResult res = con.prepareTupleQuery(sparql).evaluate()) {
	        assertTrue(res.hasNext());
	        assertEquals(0, ((Literal) res.next().getValue("a")).intValue());
	        assertTrue(res.hasNext());
	        assertEquals(1, ((Literal) res.next().getValue("a")).intValue());
	        assertTrue(res.hasNext());
	        assertEquals(0, ((Literal) res.next().getValue("a")).intValue());
	        assertTrue(res.hasNext());
	        assertEquals(1, ((Literal) res.next().getValue("a")).intValue());
	        assertFalse(res.hasNext());
        }
    }

    @Test
    public void testSES2154SubselectOptional() throws Exception {
        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        for (char c = 'a'; c < 'k'; c++) {
            con.add(vf.createIRI("http://example.com/" + c), RDF.TYPE, SCHEMA_ORG.PERSON);
        }
        String sparql = "PREFIX : <http://example.com/>\n" + "PREFIX schema: <http://schema.org/>\n" + "\n" + "SELECT (COUNT(*) AS ?count)\n" + "WHERE {\n" + "  {\n" + "    SELECT ?person\n" + "    WHERE {\n" + "      ?person a schema:Person .\n" + "    }\n" + "    LIMIT 5\n" + "  }\n" + "  OPTIONAL {\n" + "    [] :nonexistent [] .\n" + "  }\n" + "}";
        try (TupleQueryResult res = con.prepareTupleQuery(sparql).evaluate()) {
        	assertEquals(5, ((Literal) res.next().getBinding("count").getValue()).intValue());
        }
    }

    @Test (expected = QueryEvaluationException.class)
    public void testInvalidFunction() {
        try (TupleQueryResult res = con.prepareTupleQuery("PREFIX fn: <http://example.com/>\nSELECT ?whatever\nWHERE {\nBIND (fn:whatever(\"foo\") AS ?whatever)\n}").evaluate()) {
        	res.hasNext();
        }
    }

    @Test
    public void testSesameNil() throws Exception {
        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        con.add(vf.createIRI("http://a"), vf.createIRI("http://b"), vf.createIRI("http://c"));
        con.add(vf.createIRI("http://a"), vf.createIRI("http://d"), vf.createIRI("http://e"), vf.createIRI("http://f"));
        String sparql = "PREFIX sesame: <" + SESAME.NAMESPACE + ">\nSELECT (COUNT(*) AS ?count)\n" + "FROM sesame:nil WHERE {?s ?p ?o}";
        try (TupleQueryResult res = con.prepareTupleQuery(sparql).evaluate()) {
        	assertEquals(1, ((Literal) res.next().getBinding("count").getValue()).intValue());
        }
    }

    @Test
    public void testRdf4jNil() throws Exception {
        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        con.add(vf.createIRI("http://a"), vf.createIRI("http://b"), vf.createIRI("http://c"));
        con.add(vf.createIRI("http://a"), vf.createIRI("http://d"), vf.createIRI("http://e"), vf.createIRI("http://f"));
        try (TupleQueryResult res = con.prepareTupleQuery("PREFIX rdf4j: <" + RDF4J.NAMESPACE + ">\nSELECT (COUNT(*) AS ?count)\n" + "FROM rdf4j:nil WHERE {?s ?p ?o}").evaluate()) {
        	assertEquals(1, ((Literal) res.next().getBinding("count").getValue()).intValue());
        }
    }

    @Test
    public void testEmptyOptional() throws Exception {
        String q = "SELECT * WHERE {" +
            "  OPTIONAL {<https://nonexisting> <https://nonexisting> <https://nonexisting> .}" +
            "}";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
        	assertTrue(res.hasNext());
        }
    }

    @Test
    public void testEmptyOptionalSubselect() throws Exception {
        String q = "SELECT * WHERE {" +
            "  OPTIONAL { SELECT * WHERE {<https://nonexisting> <https://nonexisting> <https://nonexisting> .}}" +
            "}";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
        	assertFalse(res.hasNext());
        }
    }

    @Test
    public void testAggregates() {
    	String q = "SELECT (MAX(?x) as ?maxx) (MIN(?x) as ?minx) (AVG(?x) as ?avgx) (SUM(?x) as ?sumx) (COUNT(?x) as ?countx) (SAMPLE(?x) as ?samplex) (GROUP_CONCAT(?x) as ?concatx) { VALUES ?x {1 2 2 3} }";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals(3, ((Literal) bs.getValue("maxx")).intValue());
	        assertEquals(1, ((Literal) bs.getValue("minx")).intValue());
	        assertEquals(2, ((Literal) bs.getValue("avgx")).intValue());
	        assertEquals(4, ((Literal) bs.getValue("countx")).intValue());
	        assertEquals(8, ((Literal) bs.getValue("sumx")).intValue());
	        assertNotNull(bs.getValue("samplex"));
	        assertEquals("1 2 2 3", ((Literal) bs.getValue("concatx")).getLabel());
        }
    }

    @Test
    public void testDistinctAggregates() {
    	String q = "SELECT (MAX(distinct ?x) as ?maxx) (MIN(distinct ?x) as ?minx) (AVG(distinct ?x) as ?avgx) (SUM(distinct ?x) as ?sumx) (COUNT(distinct ?x) as ?countx) (SAMPLE(distinct ?x) as ?samplex) (GROUP_CONCAT(distinct ?x) as ?concatx) { VALUES ?x {1 2 2 3} }";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals(3, ((Literal) bs.getValue("maxx")).intValue());
	        assertEquals(1, ((Literal) bs.getValue("minx")).intValue());
	        assertEquals(2, ((Literal) bs.getValue("avgx")).intValue());
	        assertEquals(3, ((Literal) bs.getValue("countx")).intValue());
	        assertEquals(6, ((Literal) bs.getValue("sumx")).intValue());
	        assertNotNull(bs.getValue("samplex"));
	        assertEquals("1 2 3", ((Literal) bs.getValue("concatx")).getLabel());
        }
    }

    @Test
    public void testEmptyAggregate() {
    	String q = "SELECT (MAX(?x) as ?maxx) {}";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals(0, bs.size());
        }
    }

    @Test
	public void testCountAggregateWithGroupEmptyResult() {
		String q = "SELECT ?s (COUNT(?o) as ?oc) {\n" +
				"   ?s ?p ?o .\n" +
				" }\n" +
				" GROUP BY ?s\n";
		try (TupleQueryResult result = con.prepareTupleQuery(q).evaluate()) {
			assertFalse(result.hasNext());
		}
	}

    @Test
    public void testConstantAggregates() {
    	String q = "SELECT (MAX(-2) as ?maxx) (MIN(3) as ?minx) (AVG(1) as ?avgx) (SUM(7) as ?sumx) (COUNT('foo') as ?countx) (SAMPLE('bar') as ?samplex) (GROUP_CONCAT('foobar') as ?concatx) { }";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals(-2, ((Literal) bs.getValue("maxx")).intValue());
	        assertEquals(3, ((Literal) bs.getValue("minx")).intValue());
	        assertEquals(1, ((Literal) bs.getValue("avgx")).intValue());
	        assertEquals(1, ((Literal) bs.getValue("countx")).intValue());
	        assertEquals(7, ((Literal) bs.getValue("sumx")).intValue());
	        assertEquals("bar", ((Literal) bs.getValue("samplex")).getLabel());
	        assertEquals("foobar", ((Literal) bs.getValue("concatx")).getLabel());
        }
    }

    @Test
    public void testIdenticalTriplesDifferentGraphs() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1));
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1), vf.createIRI("http://whatever/graph"));
    	String q = "SELECT (count(*) as ?c) { ?s ?p ?o }";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals(2, ((Literal)bs.getValue("c")).intValue());
        }
    }

    @Test
    public void testTripleValue() {
    	String q = "SELECT (<< <http://whatever/a> <http://whatever/val> 1 >> as ?t) {}";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertTrue(bs.getValue("t").isTriple());
        }
    }

    @Test
    public void testNestedTriples1() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createTriple(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1)));
    	String q = "SELECT ?o { <http://whatever/a> <http://whatever/val> << ?s ?p ?o >> }";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals(1, ((Literal)bs.getValue("o")).intValue());
        }
    }

    @Test
    public void testNestedTriples2() {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createTriple(vf.createIRI("http://whatever/a"), vf.createIRI("http://whatever/val"), vf.createLiteral(1)));
    	String q = "SELECT ?o { ?s ?p << <http://whatever/a> <http://whatever/val> ?o >> }";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertTrue(res.hasNext());
	        BindingSet bs = res.next();
	        assertEquals(1, ((Literal)bs.getValue("o")).intValue());
        }
    }

    @Test
    public void testNonConstantPatternRegex() {
    	String q = "ASK { VALUES (?t ?p) {('abc' 'a.c')} FILTER(regex(?t, ?p)) }";
        assertTrue(con.prepareBooleanQuery(q).evaluate());
    }

    @Test
    public void testConstantIn() {
    	String q = "ASK { FILTER('b' in ('a', 'b', 'c')) }";
        assertTrue(con.prepareBooleanQuery(q).evaluate());
    }

    @Test
    public void testJoinEarlyTermination() throws Exception {
        con.add(getClass().getResource("/testdata-query/dataset-query.trig"));
    	String q = "PREFIX ex: <http://example.org/> SELECT * { ?s ex:name ?n; ex:hasParent ?p } LIMIT 4";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertEquals(4, countSlowly(res));
        }
    }

    @Test
    public void testUnionEarlyTermination() throws Exception {
        con.add(getClass().getResource("/testdata-query/dataset-query.trig"));
    	String q = "PREFIX ex: <http://example.org/> SELECT ?s { {?s ex:name ?n} UNION {?s ex:hasParent ?p} } LIMIT 4";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertEquals(4, countSlowly(res));
        }
    }

    @Test
    public void testConstruct() throws Exception {
        con.add(getClass().getResource("/testdata-query/dataset-query.trig"));
    	String q = "PREFIX ex: <http://example.org/> PREFIX foaf: <http://xmlns.com/foaf/0.1/> CONSTRUCT {ex:Person rdfs:subClassOf ex:Thing. ?p a ex:Person} WHERE { [] foaf:knows+ ?p }";
        try (GraphQueryResult res = con.prepareGraphQuery(q).evaluate()) {
	        assertEquals(4, countDistinct(res));
        }
    }

    @Test
    public void testFunctionGraph() throws Exception {
    	String q = "SELECT (COUNT(?s) as ?c) FROM <" + HALYARD.FUNCTION_GRAPH_CONTEXT + "> WHERE { ?s a <http://spinrdf.org/spin#Function> }";
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
	        assertEquals(162, ((Literal)res.next().getValue("c")).intValue());
        }
    }

    @Test
    public void testDeleteGraphUsingBindings() throws Exception {
        ValueFactory vf = con.getValueFactory();
        con.add(getClass().getResource("/testdata-query/dataset-query.trig"));
        int total;
        try (TupleQueryResult res = con.prepareTupleQuery("SELECT (COUNT(?s) as ?c) {?s ?p ?o}").evaluate()) {
	        total = ((Literal)res.next().getValue("c")).intValue();
        }
        assertEquals(47, total);
        int graphSize;
        TupleQuery gcQuery = con.prepareTupleQuery("SELECT (COUNT(?s) as ?c) {GRAPH $g {?s ?p ?o}}");
        gcQuery.setBinding("g", vf.createIRI("http://example.org/graph3"));
        try (TupleQueryResult res = gcQuery.evaluate()) {
	        graphSize = ((Literal)res.next().getValue("c")).intValue();
        }
        assertEquals(2, graphSize);
        Update update = con.prepareUpdate("DELETE WHERE {GRAPH $g {?s ?p ?o}}");
        update.setBinding("g", vf.createIRI("http://example.org/graph3"));
        update.execute();
        int totalAfterDelete;
        try (TupleQueryResult res = con.prepareTupleQuery("SELECT (COUNT(?s) as ?c) {?s ?p ?o}").evaluate()) {
	        totalAfterDelete = ((Literal)res.next().getValue("c")).intValue();
        }
        assertEquals(total-graphSize, totalAfterDelete);
    }

    @Test
    public void testFilterScoping1() throws Exception {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://example.com/a"), RDF.TYPE, SCHEMA_ORG.PERSON);
        con.add(vf.createIRI("http://example.com/b"), RDF.TYPE, SCHEMA_ORG.PERSON);
        con.add(vf.createIRI("http://example.com/b"), SCHEMA_ORG.NAME, vf.createLiteral("Claire"));
        int total;
        try (TupleQueryResult res = con.prepareTupleQuery("PREFIX s: <http://schema.org/>\nSELECT ?s {?s a s:Person FILTER EXISTS {?s s:name []} }").evaluate()) {
	        total = count(res);
        }
        assertEquals(1, total);
    }

    @Test
    public void testFilterScoping2() throws Exception {
        ValueFactory vf = con.getValueFactory();
        con.add(vf.createIRI("http://example.com/a"), RDF.TYPE, SCHEMA_ORG.PERSON);
        con.add(vf.createIRI("http://example.com/b"), RDF.TYPE, SCHEMA_ORG.PERSON);
        con.add(vf.createIRI("http://example.com/b"), SCHEMA_ORG.NAME, vf.createLiteral("Claire"));
        int total;
        try (TupleQueryResult res = con.prepareTupleQuery("PREFIX s: <http://schema.org/>\nSELECT ?s {?s a s:Person { FILTER EXISTS {?s s:name []} } }").evaluate()) {
	        total = count(res);
        }
        assertEquals(2, total);
    }

    private static int count(QueryResult<?> res) {
    	int n = 0;
    	while (res.hasNext()) {
    		res.next();
    		n++;
    	}
    	return n;
    }

    private static int countDistinct(QueryResult<?> res) {
    	Set<Object> distinct = new HashSet<>();
    	while (res.hasNext()) {
    		distinct.add(res.next());
    	}
    	return distinct.size();
    }

    private static int countSlowly(QueryResult<?> res) throws InterruptedException {
    	int num = 0;
    	while (res.hasNext()) {
    		res.next();
    		num++;
    		Thread.sleep(100L); // allow time for threads to potentially do more work than is needed
    	}
    	return num;
    }
}
