package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algorithms;
import com.msd.gin.halyard.algebra.NAryTupleOperator;
import com.msd.gin.halyard.algebra.StarJoin;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.TupleQueryResultBuilder;
import org.eclipse.rdf4j.query.resultio.QueryResultIO;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class HalyardStrategyJoinTest {
	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		List<Object[]> testValues = new ArrayList<>();
		testValues.add(new Object[] {"Nested", 0, 0, Float.MAX_VALUE, Integer.MAX_VALUE});
		testValues.add(new Object[] {"Hash", Integer.MAX_VALUE, Integer.MAX_VALUE, 0.0f, Integer.MAX_VALUE});
		testValues.add(new Object[] {"Partitioned hash", Integer.MAX_VALUE, 1, 0.0f, Integer.MAX_VALUE});
		testValues.add(new Object[] {"Star", 0, 0, Float.MAX_VALUE, 1});
		return testValues;
	}

	private final int optHashJoinLimit;
	private final int evalHashJoinLimit;
    private final float cardinalityRatio;
    private final int starJoinMin;
    private Repository repo;
    private RepositoryConnection con;
    private MemoryStoreWithHalyardStrategy strategy;

    public HalyardStrategyJoinTest(String algo, int optHashJoinLimit, int evalHashJoinLimit, float cardinalityRatio, int starJoinMin) {
		this.optHashJoinLimit = optHashJoinLimit;
		this.evalHashJoinLimit = evalHashJoinLimit;
		this.cardinalityRatio = cardinalityRatio;
		this.starJoinMin = starJoinMin;
    }

    @Before
    public void setUp() throws Exception {
    	strategy = new MemoryStoreWithHalyardStrategy(optHashJoinLimit, evalHashJoinLimit, cardinalityRatio, starJoinMin, Integer.MAX_VALUE, 0);
        repo = new SailRepository(strategy);
        repo.init();
        con = repo.getConnection();
    }

    @After
    public void tearDown() throws Exception {
        con.close();
        repo.shutDown();
    }

    private String expectedAlgo() {
    	if (optHashJoinLimit == 0) {
    		return Algorithms.NESTED_LOOPS;
    	} else {
    		return Algorithms.HASH_JOIN;
    	}
    }

    @Test
    public void testJoinOn1Var() throws Exception {
        String q = "prefix : <http://example/> select ?s ?t where {?s :r/:s ?t}";
        joinTest(q, "/test-cases/join-results-1.srx", 1, expectedAlgo());
    }

    @Test
    public void testJoinOn1Var_duplicate() throws Exception {
        String q = "prefix : <http://example/> select ?x where {:x2 :p ?x. :x2 :p ?x}";
        joinTest(q, "/test-cases/join-results-1-duplicate.srx", 1, starJoinMin == 1 ? Algorithms.STAR_JOIN : expectedAlgo());
    }

    @Test
    public void testJoinOn1Var_bound() throws Exception {
        String q = "prefix : <http://example/> select ?s ?t where {values ?x {:y2 :y3 :y3a} ?s :r ?x. ?x :s ?t}";
        joinTest(q, "/test-cases/join-data.ttl", "/test-cases/join-results-1.srx", 2, Algorithms.NESTED_LOOPS, expectedAlgo());
    }

    @Test
    public void testJoinOn2Var() throws Exception {
        String q = "prefix : <http://example/> select ?x ?y where {?x :p ?y. ?x :t ?y}";
        joinTest(q, "/test-cases/join-results-2.srx", 1, starJoinMin == 1 ? Algorithms.STAR_JOIN : expectedAlgo());
    }

    @Test
    public void testJoinOn2Var_duplicate() throws Exception {
        String q = "prefix : <http://example/> select ?x ?y where {?x :s ?y. ?x :s ?y}";
        joinTest(q, "/test-cases/join-results-2-duplicate.srx", 1, starJoinMin == 1 ? Algorithms.STAR_JOIN : expectedAlgo());
    }

    @Test
    public void testJoinOn2Var_constant() throws Exception {
        String q = "prefix : <http://example/> select ?x ?y where {:y3 :s ?x, ?y}";
        joinTest(q, "/test-cases/join-results-2-constant.srx", 1, starJoinMin == 1 ? Algorithms.STAR_JOIN : expectedAlgo());
    }

    @Test
    public void testJoinOn0Var() throws Exception {
        String q = "prefix : <http://example/> select * where {?s :r ?t. ?x :s ?y}";
        joinTest(q, "/test-cases/join-results-0.srx", 1, expectedAlgo());
    }

    @Test
    public void testJoinOn0Var_empty() throws Exception {
        String q = "prefix : <http://example/> select * where {:x1 :q \"a\". ?x :p ?y}";
        joinTest(q, "/test-cases/join-results-empty-0.srx", 1, expectedAlgo());
    }

    @Test
    public void testJoin_empty_left() throws Exception {
        String q = "prefix : <http://example/> select * where {:x1 :z ?y. ?y :p ?v}";
        joinTest(q, "/test-cases/join-results-empty.srx", 1, expectedAlgo());
    }

    @Test
    public void testJoin_empty_right() throws Exception {
        String q = "prefix : <http://example/> select * where {:x1 :p ?y. ?y :z ?v}";
        joinTest(q, "/test-cases/join-results-empty.srx", 1, expectedAlgo());
    }

    @Test
    public void testStarJoin_empty_bound() throws Exception {
        String q = "prefix : <http://example/> select * where {:x1 :p ?y. :x1 :z ?v}";
        joinTest(q, "/test-cases/join-results-empty.srx", 1, starJoinMin == 1 ? Algorithms.STAR_JOIN : expectedAlgo());
    }

    @Test
    public void testStarJoin_empty_unbound() throws Exception {
        String q = "prefix : <http://example/> select * where {?x :p ?y. ?x :z ?v}";
        joinTest(q, "/test-cases/join-results-empty.srx", 1, starJoinMin == 1 ? Algorithms.STAR_JOIN : expectedAlgo());
    }

    @Test
    public void testStarJoin() throws Exception {
        String q = "prefix : <http://example/> select * where {?x :p ?u. ?x :r ?v}";
        joinTest(q, "/test-cases/star-join-results.srx", 1, starJoinMin == 1 ? Algorithms.STAR_JOIN : expectedAlgo());
    }

    @Test
	public void testJoin_unbound_value() throws Exception {
		String q = "prefix : <http://example/> "
				+ "select ?x ?y where { "
				+ "  values (?x ?y) { (undef 22) } "
				+ "  ?x :p ?y"
				+ "}";
		joinTest(q, "/test-cases/join-results-unbound-value.srx", 1, expectedAlgo());
    }


    @Test
    public void testLeftJoinOn1Var() throws Exception {
        String q = "prefix : <http://example/> select ?s ?t where {?s :r ?k. optional {?k :s ?t} }";
        joinTest(q, "/test-cases/left-join-results-1.srx", 1, expectedAlgo());
    }

    @Test
    public void testLeftJoinOn2Var() throws Exception {
        String q = "prefix : <http://example/> select ?x ?y where {?x :p ?y. optional {?x :t ?y} }";
        joinTest(q, "/test-cases/left-join-results-2.srx", 1, expectedAlgo());
    }

    @Test
    public void testLeftJoinOn0Var() throws Exception {
        String q = "prefix : <http://example/> select * where {?s :r ?t. optional {?x :s ?y} }";
        joinTest(q, "/test-cases/left-join-results-0.srx", 1, expectedAlgo());
    }

    @Test
    public void testLeftJoinOn0Var_empty() throws Exception {
        String q = "prefix : <http://example/> select * where {:x1 :q \"a\". optional {?x :p ?y} }";
        joinTest(q, "/test-cases/left-join-results-empty-0.srx", 1, expectedAlgo());
    }

    @Test
    public void testLeftJoinOn0Var_empty_swapped() throws Exception {
        String q = "prefix : <http://example/> select * where {?x :p ?y. optional {:x1 :q \"a\"} }";
        joinTest(q, "/test-cases/left-join-results-empty-0.srx", 1, expectedAlgo());
    }

    @Test
    public void testLeftJoin_not_exists() throws Exception {
        String q = "prefix : <http://example/> select * where {?x :p ?y. optional {?x :notexists []} }";
        joinTest(q, "/test-cases/left-join-results-empty-0.srx", 1, expectedAlgo());
    }

	@Test
	public void testLefJoin_unbound_value() throws Exception {
		String q = "prefix : <http://example/> "
				+ "select ?x ?y where { "
				+ "  values (?x ?y) { (undef 22) } "
				+ "  optional { "
				+ "    ?x :p ?y "
				+ "  }"
				+ "}";
		joinTest(q, "/test-cases/left-join-results-unbound-value.srx", 1, expectedAlgo());
	}


    @Test
    public void testBadNestedLeftJoin() throws Exception {
        String q = "prefix : <http://example/> select ?x ?y ?z where {?x :name 'paul'. optional {?y :name 'george'. optional {?x :email ?z} } }";
        joinTest(q, "/test-cases/cs-0605124.ttl", "/test-cases/cs-0605124-ex3.srx", 2, expectedAlgo(), expectedAlgo());
    }

    @Test
    public void testNoncommutativeAnd1() throws Exception {
        String q = "prefix : <http://example/> select ?x ?y ?z where {?x :name 'paul'. {?y :name 'george'. optional {?x :email ?z}} }";
        joinTest(q, "/test-cases/cs-0605124.ttl", "/test-cases/cs-0605124-ex4.srx", 2, Algorithms.HASH_JOIN, expectedAlgo());
    }

    @Test
    public void testNoncommutativeAnd2() throws Exception {
        String q = "prefix : <http://example/> select ?x ?y ?z where {{?y :name 'george'. optional {?x :email ?z}} ?x :name 'paul'. }";
        joinTest(q, "/test-cases/cs-0605124.ttl", "/test-cases/cs-0605124-ex4.srx", 2, Algorithms.HASH_JOIN, expectedAlgo());
    }

    @Test
	public void testCartesianProduct() throws Exception {
		String q = ""
				+ "select ?x ?y where { "
				+ "  values ?x { undef 67 } "
				+ "  values ?y { undef 42 } "
				+ "}";
		joinTest(q, "/test-cases/cartesian-product-results.srx", 1, expectedAlgo());
	}


    private void joinTest(String q, String expectedOutput, int expectedJoins, String expectedAlgo) throws Exception {
    	joinTest(q, "/test-cases/join-data.ttl", expectedOutput, expectedJoins, expectedAlgo != null ? new String[] {expectedAlgo} : null);
    }

    private void joinTest(String q, String data, String expectedOutput, int expectedJoins, String... expectedAlgos) throws Exception {
    	if (expectedJoins == 0 && expectedAlgos != null) {
    		throw new IllegalArgumentException("No join expected");
    	}

    	con.add(getClass().getResource(data));
        Set<BindingSet> results;
        try (TupleQueryResult res = con.prepareTupleQuery(q).evaluate()) {
            results = toSet(res);
        }
        Set<BindingSet> expectedResults;
        try (InputStream in = getClass().getResourceAsStream(expectedOutput)) {
        	TupleQueryResultBuilder tqrBuilder = new TupleQueryResultBuilder();
        	QueryResultIO.parseTuple(in, TupleQueryResultFormat.SPARQL, tqrBuilder, repo.getValueFactory());
            try (TupleQueryResult res = tqrBuilder.getQueryResult()) {
                expectedResults = toSet(res);
            }
        }
        TupleExpr expr = strategy.getQueryHistory().getLast();
        assertEquals(expr.toString(), expectedResults, results);

        List<TupleExpr> joins = new ArrayList<>();
        expr.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>() {
			@Override
			public void meet(Join node) {
				joins.add(node);
				super.meet(node);
			}
			@Override
			public void meet(LeftJoin node) {
				joins.add(node);
				super.meet(node);
			}
        	@Override
        	public void meet(StarJoin node) {
				joins.add(node);
				super.meet(node);
        	}
        });
        assertEquals(expectedJoins, joins.size());
        if (!joins.isEmpty()) {
	        for (int i=0; i<joins.size(); i++) {
	        	TupleExpr join = joins.get(i);
	        	assertEquals(expr.toString(), expectedAlgos[i], getJoinAlgorithm(join));
	        }
	        assertEquals(expr.toString(), expectedResults.size(), joins.get(0).getResultSizeActual());
        }
    }

    private static String getJoinAlgorithm(TupleExpr join) {
    	if (join instanceof BinaryTupleOperator) {
    		return ((BinaryTupleOperator) join).getAlgorithmName();
    	} else if (join instanceof NAryTupleOperator) {
    		return ((NAryTupleOperator) join).getAlgorithmName();
    	} else {
    		throw new AssertionError();
    	}
    }

    private static Set<BindingSet> toSet(TupleQueryResult res) {
        Set<BindingSet> results = new HashSet<>();
        while (res.hasNext()) {
            BindingSet bs = res.next();
            results.add(bs);
        }
        return results;
    }
}
