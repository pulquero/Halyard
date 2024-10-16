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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author Adam Sotona (MSD)
 */
@RunWith(Parameterized.class)
public class HalyardEvaluationStatisticsTest {
	private static final double S = SimpleStatementPatternCardinalityCalculator.SUBJECT_VAR_CARDINALITY;
	private static final double P = SimpleStatementPatternCardinalityCalculator.PREDICATE_VAR_CARDINALITY;
	private static final double O = SimpleStatementPatternCardinalityCalculator.OBJECT_VAR_CARDINALITY;
	private static final double PRI = HalyardEvaluationStatistics.PRIORITY_VAR_FACTOR;
	private static final double CF = ExtendedEvaluationStatistics.COMPLETENESS_FACTOR;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"select * where {?s a ?o}",            S*O, null, null},
            {"select * where {?s ?p ?o}",           S*P*O, null, null},
            {"select * where {?s a \"1\"}",         S, null, null},
            {"select * where {?a ?b ?c; ?d ?e}",    S*P*O*P*O, null, null},
            {"select * where {?a a ?c; ?d ?e}",     S*O*P*O, null, null},
            {"select * where {?a ?b ?c; a ?e}",     S*P*O*O, null, null},
            {"select * where {?s a ?o}",            S, new String[]{"o"}, null},
            {"select * where {?s ?p ?o}",           P, new String[]{"s", "o"}, null},
            {"select * where {?s a \"1\"}",         1.0,   new String[]{"s"}, null},
            {"select * where {?a ?b ?c; ?d ?e}",    S*P*O, new String[]{"b", "c"}, null},
            {"select * where {?a a ?c; ?d ?e}",     S*O, new String[]{"d", "c"}, null},
            {"select * where {?a ?b ?c; a ?e}",     S, new String[]{"b", "e", "c"}, null},
            {"select * where {{?a a \"1\". optional {?a a ?b}} union {?a a \"2\"}}", S*O*CF + S, null, null},
            {"select * where {?s a \"1\"^^<" + HALYARD.SEARCH + ">}",           1.0E-4,   new String[]{"s"}, null},
            {"select * where {?a ?b ?c}",           P*O/PRI, new String[]{"a"}, new String[]{"a"}},
            {"select * where {?a ?b ?c}",           O/PRI/PRI, new String[]{"a", "b"}, new String[]{"a", "b"}},
            {"select * where {?a ?b ?c}",           1/PRI/PRI/PRI, new String[]{"a", "b" , "c"}, new String[]{"a", "b", "c"}},
            {"select * where {?a ?b ?c}",           S*P*O/PRI, null, new String[]{"a"}},
            {"select * where {?a ?b ?c}",           S*P*O/PRI/PRI, null, new String[]{"a", "b"}},
            {"select * where {?s ?p ?o filter(?o != 1)}", S*P*O*CF, null, null},
            {"select * where {values ?s {1 2 3}}", 3.0, null, null},
            {"select * where {}", 1.0, null, null},
            {"select * where {service <http://remote> {?s ?p ?o}}", 103.0, null, null},
            {"select * where {?s <:p>* ?o}", 2*S*P*O, null, null},
            {"select * where {<< <:s> <:p> <:o> >> ?p ?o}", P*O, null, null},
        });
    }

    private final String query;
    private final double cardinality;
    private final Set<String> boundVars = new HashSet<>();
    private final Set<String> priorityVars = new HashSet<>();

    public HalyardEvaluationStatisticsTest(String query, double cardinality, String[] boundVars, String[] priorityVars) {
        this.query = query;
        this.cardinality = cardinality;
        if (boundVars != null) {
            this.boundVars.addAll(Arrays.asList(boundVars));
        }
        if (priorityVars != null) {
            this.priorityVars.addAll(Arrays.asList(priorityVars));
        }
    }

	private HalyardEvaluationStatistics createStatistics() {
		return new HalyardEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY, null);
	}

	@Test
	public void testCardinality() {
		QueryRoot queryRoot = (QueryRoot) new SPARQLParser().parseQuery(query, "http://baseuri/").getTupleExpr();
		Assert.assertEquals(query, cardinality, createStatistics().getCardinality(queryRoot, boundVars, priorityVars, false), cardinality/1000000.0);
		Map<TupleExpr,Double> mapToUpdate = new HashMap<>();
		createStatistics().updateCardinalityMap(queryRoot, boundVars, priorityVars, mapToUpdate, false);
		Assert.assertEquals(query, cardinality, mapToUpdate.get(queryRoot.getArg()), cardinality/1000000.0);
		TupleExprCounter counter = new TupleExprCounter();
		queryRoot.getArg().visit(counter);
		Assert.assertEquals(counter.count, mapToUpdate.size());
		for (TupleExpr node : mapToUpdate.keySet()) {
			Map<TupleExpr,Double> individualMapToUpdate = new HashMap<>();
			createStatistics().updateCardinalityMap(node, boundVars, priorityVars, individualMapToUpdate, false);
			Assert.assertFalse(individualMapToUpdate.isEmpty());
		}
	}


	static class TupleExprCounter extends AbstractExtendedQueryModelVisitor<RuntimeException> {
		int count;
		boolean inService;

		@Override
		public void meet(ArbitraryLengthPath alp) {
			count++;
		}

		@Override
		public void meet(Service node) {
			count++;
			inService = true;
			super.meet(node);
			inService = false;
		}

		@Override
		protected void meetNode(QueryModelNode node) {
			if (!inService) {
				if (node instanceof TupleExpr) {
					count++;
				}
			}
			super.meetNode(node);
		}
	}
}
