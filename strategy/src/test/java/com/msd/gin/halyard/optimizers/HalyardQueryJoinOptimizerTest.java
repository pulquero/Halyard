/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.StarJoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardQueryJoinOptimizerTest {
	private static final String BASE_URI = "http://baseuri/";

	private HalyardEvaluationStatistics createStatistics() {
		return new HalyardEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY, null);
	}

	@Test
    public void testQueryJoinOptimizerWithSimpleJoin() {
        final TupleExpr expr = new SPARQLParser().parseQuery("select * where {?a ?b ?c, \"1\".}", BASE_URI).getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertTrue(expr.toString(), joinOrder.list.get(0).getObjectVar().hasValue());
        assertEquals(expr.toString(), "c", joinOrder.list.get(1).getObjectVar().getName());
    }

    @Test
    public void testQueryJoinOptimizerWithSplitFunction() {
        final TupleExpr expr = new SPARQLParser().parseQuery("select * where {?a a \"1\";?b ?d. filter (<" + HALYARD.PARALLEL_SPLIT_FUNCTION + ">(10, ?d))}", BASE_URI).getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertEquals(expr.toString(), "d", joinOrder.list.get(0).getObjectVar().getName());
        assertTrue(expr.toString(), joinOrder.list.get(1).getObjectVar().hasValue());
    }

    @Test
    public void testQueryJoinOptimizerWithSplitFunctionOutsideSubquery() {
        final TupleExpr expr = new SPARQLParser().parseQuery("select * where {?a a \"1\";?b ?d. filter (<" + HALYARD.PARALLEL_SPLIT_FUNCTION + ">(10, ?d)) { select * {?d a ?type; rdfs:label ?l} }}", BASE_URI).getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertEquals(expr.toString(), "type", joinOrder.list.get(0).getObjectVar().getName());
        double expected = SimpleStatementPatternCardinalityCalculator.SUBJECT_VAR_CARDINALITY*SimpleStatementPatternCardinalityCalculator.OBJECT_VAR_CARDINALITY;
        assertEquals(expr.toString(), expected, joinOrder.list.get(0).getResultSizeEstimate(), 0.0);
    }

    @Test
    public void testQueryJoinOptimizerWithSplitFunctionInsideSubquery() {
        final TupleExpr expr = new SPARQLParser().parseQuery("select * where { ?d a ?type; rdfs:label ?l { select * {?a a \"1\";?b ?d. filter (<" + HALYARD.PARALLEL_SPLIT_FUNCTION + ">(10, ?d))} }}", BASE_URI).getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertEquals(expr.toString(), "d", joinOrder.list.get(0).getObjectVar().getName());
        double expected = SimpleStatementPatternCardinalityCalculator.SUBJECT_VAR_CARDINALITY*SimpleStatementPatternCardinalityCalculator.PREDICATE_VAR_CARDINALITY*SimpleStatementPatternCardinalityCalculator.OBJECT_VAR_CARDINALITY/HalyardEvaluationStatistics.PRIORITY_VAR_FACTOR;
        assertEquals(expr.toString(), expected, joinOrder.list.get(0).getResultSizeEstimate(), 0.0);
    }

    @Test
    public void testQueryJoinOptimizerWithBind() {
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { BIND (<http://whatever/obj> AS ?b)  ?a <http://whatever/pred> ?b, \"whatever\" .}", BASE_URI).getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertEquals(expr.toString(), "b", joinOrder.list.get(0).getObjectVar().getName());
    }

    @Test
    public void testQueryJoinOptimizerWithBind2() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { BIND (<http://whatever/obj> AS ?c)  ?a <http://whatever/1> ?b . ?b <http://whatever/2> ?c.}", BASE_URI).getTupleExpr();
        new HalyardQueryJoinOptimizer(createStatistics()).optimize(expr, null, null);
        IRI pred1 = vf.createIRI("http://whatever/1");
        IRI pred2 = vf.createIRI("http://whatever/2");
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertEquals(expr.toString(), Arrays.asList(pred2, pred1), joinOrder.predicates);
    }

    @Test
    public void testQueryJoinOptimizerWithStats1() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { ?a <http://whatever/1>/<http://whatever/2>/<http://whatever/3> ?b }", BASE_URI).getTupleExpr();
        IRI pred1 = vf.createIRI("http://whatever/1");
        IRI pred2 = vf.createIRI("http://whatever/2");
        IRI pred3 = vf.createIRI("http://whatever/3");
        Map<IRI, Double> predicateStats = new HashMap<>();
        predicateStats.put(pred1, 100.0);
        predicateStats.put(pred2, 5.0);
        predicateStats.put(pred3, 25.0);
        new HalyardQueryJoinOptimizer(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), null)).optimize(expr, null, null);
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertEquals(expr.toString(), Arrays.asList(pred2, pred3, pred1), joinOrder.predicates);
    }

    @Test
    public void testQueryJoinOptimizerWithStats2() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { ?a <http://whatever/1>/<http://whatever/2> ?b. ?a <http://whatever/a>/<http://whatever/b> ?c }", BASE_URI).getTupleExpr();
        IRI pred1 = vf.createIRI("http://whatever/1");
        IRI pred2 = vf.createIRI("http://whatever/2");
        IRI preda = vf.createIRI("http://whatever/a");
        IRI predb = vf.createIRI("http://whatever/b");
        Map<IRI, Double> predicateStats = new HashMap<>();
        predicateStats.put(pred1, 100.0);
        predicateStats.put(pred2, 5.0);
        predicateStats.put(preda, 2.0);
        predicateStats.put(predb, 45.0);
        new HalyardQueryJoinOptimizer(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), null)).optimize(expr, null, null);
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertEquals(expr.toString(), Arrays.asList(preda, pred1, pred2, predb), joinOrder.predicates);
    }

    @Test
    public void testQueryJoinOptimizerWithStarJoin() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { ?a <http://whatever/1> ?b; <http://whatever/2> ?c; <http://whatever/3> ?d }", BASE_URI).getTupleExpr();
        IRI pred1 = vf.createIRI("http://whatever/1");
        IRI pred2 = vf.createIRI("http://whatever/2");
        IRI pred3 = vf.createIRI("http://whatever/3");
        Map<IRI, Double> predicateStats = new HashMap<>();
        predicateStats.put(pred1, 100.0);
        predicateStats.put(pred2, 5.0);
        predicateStats.put(pred3, 8.0);
        new StarJoinOptimizer(2).optimize(expr, null, null);
        new HalyardQueryJoinOptimizer(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), null)).optimize(expr, null, null);
        List<StarJoin> sjs = new ArrayList<>();
        expr.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>(){
            @Override
            public void meet(StarJoin node) {
            	sjs.add(node);
            	super.meet(node);
            }
        });
        assertEquals(expr.toString(), 1, sjs.size());
        assertEquals(expr.toString(), Arrays.asList(pred2, pred3, pred1),  sjs.get(0).getArgs().stream().map(sp -> ((StatementPattern)sp).getPredicateVar().getValue()).collect(Collectors.toList()));
    }

    @Test
    public void testQueryJoinOptimizerWithService() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { ?s a <http://whatever/Thing>. SERVICE <http://endpoint> { ?s <http://whatever/a> ?a; <http://whatever/b> ?b } }", BASE_URI).getTupleExpr();
        IRI preda = vf.createIRI("http://whatever/a");
        IRI predb = vf.createIRI("http://whatever/b");
        Map<IRI, Double> predicateStats = new HashMap<>();
        predicateStats.put(RDF.TYPE, 100.0);
        predicateStats.put(preda, 5.0);
        predicateStats.put(predb, 2.0);
        new HalyardQueryJoinOptimizer(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), serviceUrl -> {
        	assertEquals("http://endpoint", serviceUrl);
        	return Optional.of(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), null));
        })).optimize(expr, null, null);
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertEquals(expr.toString(), Arrays.asList(predb, preda, RDF.TYPE), joinOrder.predicates);
    }

    @Test
    public void testQueryJoinOptimizerWithServiceOnly() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { SERVICE <http://endpoint> { ?a <http://whatever/1>/<http://whatever/2> ?b. ?a <http://whatever/a>/<http://whatever/b> ?c } }", BASE_URI).getTupleExpr();
        IRI pred1 = vf.createIRI("http://whatever/1");
        IRI pred2 = vf.createIRI("http://whatever/2");
        IRI preda = vf.createIRI("http://whatever/a");
        IRI predb = vf.createIRI("http://whatever/b");
        Map<IRI, Double> predicateStats = new HashMap<>();
        predicateStats.put(pred1, 100.0);
        predicateStats.put(pred2, 5.0);
        predicateStats.put(preda, 2.0);
        predicateStats.put(predb, 45.0);
        new HalyardQueryJoinOptimizer(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(null), serviceUrl -> {
        	assertEquals("http://endpoint", serviceUrl);
        	return Optional.of(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), null));
        })).optimize(expr, null, null);
        JoinOrderVisitor joinOrder = new JoinOrderVisitor(expr);
        assertEquals(expr.toString(), Arrays.asList(preda, pred1, pred2, predb), joinOrder.predicates);
    }

    @Test
    public void testQueryJoinOptimizerWithServiceStarJoin() {
    	ValueFactory vf = SimpleValueFactory.getInstance();
        final TupleExpr expr = new SPARQLParser().parseQuery("SELECT * WHERE { SERVICE <http://endpoint> { ?a <http://whatever/1> ?b; <http://whatever/2> ?c; <http://whatever/3> ?d } }", BASE_URI).getTupleExpr();
        IRI pred1 = vf.createIRI("http://whatever/1");
        IRI pred2 = vf.createIRI("http://whatever/2");
        IRI pred3 = vf.createIRI("http://whatever/3");
        Map<IRI, Double> predicateStats = new HashMap<>();
        predicateStats.put(pred1, 100.0);
        predicateStats.put(pred2, 5.0);
        predicateStats.put(pred3, 8.0);
        new StarJoinOptimizer(2).optimize(expr, null, null);
        new HalyardQueryJoinOptimizer(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(null), serviceUrl -> {
        	assertEquals("http://endpoint", serviceUrl);
        	return Optional.of(new HalyardEvaluationStatistics(() -> new MockStatementPatternCardinalityCalculator(predicateStats), null));
        })).optimize(expr, null, null);
        List<StarJoin> sjs = new ArrayList<>();
        expr.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>() {
            @Override
            public void meet(StarJoin node) {
            	sjs.add(node);
            	super.meet(node);
            }
        });
        assertEquals(expr.toString(), 1, sjs.size());
        assertEquals(expr.toString(), Arrays.asList(pred2, pred3, pred1),  sjs.get(0).getArgs().stream().map(sp -> ((StatementPattern)sp).getPredicateVar().getValue()).collect(Collectors.toList()));
    }


    private static class JoinOrderVisitor extends AbstractExtendedQueryModelVisitor<RuntimeException> {
        final List<StatementPattern> list = new ArrayList<>();
        final List<IRI> predicates = new ArrayList<>();

        JoinOrderVisitor(TupleExpr expr) {
        	expr.visit(this);
        }

        @Override
        public void meet(Join node) {
            if (node.getLeftArg() instanceof StatementPattern) {
            	StatementPattern sp = (StatementPattern)node.getLeftArg();
            	list.add(sp);
                predicates.add((IRI) sp.getPredicateVar().getValue());
            } else {
            	node.getLeftArg().visit(this);
            }
            if (node.getRightArg() instanceof StatementPattern) {
            	StatementPattern sp = (StatementPattern)node.getRightArg();
            	list.add(sp);
                predicates.add((IRI) sp.getPredicateVar().getValue());
            } else {
            	node.getRightArg().visit(this);
            }
        }
    }


    public static class MockStatementPatternCardinalityCalculator extends SimpleStatementPatternCardinalityCalculator {
		final Map<IRI, Double> predicateStats;
	
		public MockStatementPatternCardinalityCalculator(Map<IRI, Double> predicateStats) {
			this.predicateStats = predicateStats;
		}
	
		@Override
		public double getStatementCardinality(Var subjVar, Var predVar, Var objVar, Var ctxVar, Collection<String> boundVars) {
			boolean sv = hasValue(subjVar, boundVars);
			boolean ov = hasValue(objVar, boundVars);
			IRI predicate = (IRI) predVar.getValue();
			double card = predicateStats.get(predicate);
			if (sv && ov) {
				card = 1.0;
			} else if (sv || ov) {
				card = Math.sqrt(card);
			}
			return card;
		}
	}
}
