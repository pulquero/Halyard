package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.model.TupleLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.strategy.aggregators.ExtendedAggregateCollector;
import com.msd.gin.halyard.strategy.aggregators.HIndexAggregateFactory;
import com.msd.gin.halyard.strategy.aggregators.MaxWithAggregateFactory;
import com.msd.gin.halyard.strategy.aggregators.MinWithAggregateFactory;
import com.msd.gin.halyard.strategy.aggregators.ModeAggregateFactory;
import com.msd.gin.halyard.strategy.aggregators.ThreadSafeAggregateFunction;
import com.msd.gin.halyard.strategy.aggregators.TopNWithAggregateFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.MathExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.aggregate.stdev.PopulationStandardDeviationAggregateFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.function.aggregate.stdev.StandardDeviationAggregateFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.function.aggregate.variance.PopulationVarianceAggregateFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.function.aggregate.variance.VarianceAggregateFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.util.MathUtil;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunction;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.AggregateFunctionFactory;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.CustomAggregateFunctionRegistry;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HalyardCustomAggregateFunctionEvaluationTest {

	private static SailRepository rep;

	private static AggregateFunctionFactory functionFactory;

	@BeforeAll
	public static void setUp() throws IOException {
		rep = new SailRepository(new MockSailWithHalyardStrategy());
		functionFactory = new AggregateFunctionFactory() {
			@Override
			public String getIri() {
				return "https://www.rdf4j.org/aggregate#x";
			}

			@Override
			public AggregateFunction<SumCollector, Value> buildFunction(Function<BindingSet, Value> evaluationStep) {
				return new ThreadSafeAggregateFunction<>() {

					public void processAggregate(BindingSet s, Predicate<Value> distinctValue, SumCollector sum, QueryValueStepEvaluator evaluationStep)
							throws QueryEvaluationException {
						if (sum.typeErrorRef.get() != null) {
							// halt further processing if a type error has been raised
							return;
						}

						Value v = evaluationStep.apply(s);
						if (v instanceof Literal) {
							if (distinctValue.test(v)) {
								Literal nextLiteral = (Literal) v;
								if (nextLiteral.getDatatype() != null
										&& XMLDatatypeUtil.isNumericDatatype(nextLiteral.getDatatype())) {
									sum.valueRef.accumulateAndGet(nextLiteral, (x,y) -> MathUtil.compute(x, y, MathExpr.MathOp.PLUS));
								} else {
									sum.typeErrorRef.set(new ValueExprEvaluationException("not a number: " + v));
								}
							}
						}
					}
				};
			}

			@Override
			public SumCollector getCollector() {
				return new SumCollector();
			}
		};
		CustomAggregateFunctionRegistry.getInstance().add(functionFactory);
		// add data to avoid processing it every time
		addData();
	}

	@AfterAll
	public static void cleanUp() {
		CustomAggregateFunctionRegistry.getInstance().remove(functionFactory);
	}

	@Test
	public void testCustomFunction_Sum() {
		String query = "select (<" + functionFactory.getIri() + ">(?o) as ?m) where { \n"
				+ "\t?s <urn:n> ?o . filter(?o > 0) }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("125.933564200001");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_Stdev_Population() {
		String query = "select (<" + new PopulationStandardDeviationAggregateFactory().getIri()
				+ ">(?o) as ?m) where { \n"
				+ "\t?s <urn:n> ?o . filter(?o > 0) }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("12.194600810623243");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_Stdev_Default() {
		String query = "select (<" + new StandardDeviationAggregateFactory().getIri() + ">(?o) as ?m) where { \n"
				+ "\t?s <urn:n> ?o . filter(?o > 0) }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("13.0365766290937");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_StdevEmpty() {
		String query = "select (<" + new StandardDeviationAggregateFactory().getIri() + ">(?o) as ?m) where { \n"
				+ "\t?s <urn:n3> ?o . filter(?o > 0) }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_Variance() {
		String query = "select (<" + new VarianceAggregateFactory().getIri() + ">(?o) as ?m) where { \n"
				+ "\t?s <urn:n> ?o . filter(?o > 0) }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("169.95233020623206");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_VariancePopulation() {
		String query = "select (<" + new PopulationVarianceAggregateFactory().getIri() + ">(?o) as ?m) where { \n"
				+ "\t?s <urn:n> ?o . filter(?o > 0) }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("148.70828893045305");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_VarianceEmpty() {
		String query = "select (<" + new VarianceAggregateFactory().getIri() + ">(?o) as ?m) where { \n"
				+ "\t?s <urn:n3> ?o . filter(?o > 0) }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_VarianceWithDistinct() {
		String query = "select (<" + new PopulationVarianceAggregateFactory().getIri()
				+ ">(distinct ?o) as ?m) ?s where { \n"
				+ "\t ?s ?p ?o . } group by ?s order by ?s";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.002500019073522708");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book1");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book2");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book3");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book4");
				bs = result.next();
				assertThat(bs.getValue("m")).isNull();
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book5");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book6");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("1.0572322555240015");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book7");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book8");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_VarianceWithDistinct_WithStdv() {
		String query = "select (<" + new PopulationVarianceAggregateFactory().getIri() + ">(distinct ?o) as ?m) (<"
				+ new StandardDeviationAggregateFactory().getIri() + ">(?o) as ?n) ?s where { \n"
				+ "\t ?s ?p ?o . } group by ?s order by ?s";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				var bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.002500019073522708");
				assertThat(bs.getValue("n").stringValue()).isEqualTo("0.057735247160611895");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book1");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("n").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book2");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("n").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book3");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("n").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book4");
				bs = result.next();
				assertThat(bs.getValue("m")).isNull();
				assertThat(bs.getValue("n")).isNull();
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book5");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("n").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book6");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("1.0572322555240015");
				assertThat(bs.getValue("n").stringValue()).isEqualTo("1.45411984067614");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book7");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("n").stringValue()).isEqualTo("0.0");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book8");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_SumWithDistinct() {
		String query = "select (<" + functionFactory.getIri() + ">(distinct ?o) as ?m) ?s where { \n"
				+ "\t ?s <urn:n> ?o . } group by ?s order by ?s";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("12.5");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book1");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("6");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book2");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("12.5");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book3");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("31.3");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book4");
				bs = result.next();
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book5");
				assertThat(bs.getValue("m")).isNull();
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.090000200001");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book6");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("60.543564");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book7");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("3");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book8");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_MultipleSum() {
		String query = "select (<" + functionFactory.getIri() + ">(?o) as ?m) (sum(?o) as ?sa) where { \n"
				+ "\t?s <urn:n> ?o . filter(?o > 0) }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo(bs.getValue("sa").stringValue());
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_MultipleSumWithDistinct() {
		String query = "select (<" + functionFactory.getIri() + ">(distinct ?o) as ?m) (sum(?o) as ?sa) ?s where { \n"
				+ "\t?s ?p ?o . filter(?o > 0) } group by ?s order by ?s";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("25.1");
				assertThat(bs.getValue("sa").stringValue()).isEqualTo("37.6");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book1");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("6");
				assertThat(bs.getValue("sa").stringValue()).isEqualTo("6");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book2");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("12.5");
				assertThat(bs.getValue("sa").stringValue()).isEqualTo("12.5");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book3");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("31.3");
				assertThat(bs.getValue("sa").stringValue()).isEqualTo("31.3");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book4");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("311");
				assertThat(bs.getValue("sa").stringValue()).isEqualTo("311");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book5");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("0.090000200001");
				assertThat(bs.getValue("sa").stringValue()).isEqualTo("0.090000200001");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book6");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("60.543564");
				assertThat(bs.getValue("sa").stringValue()).isEqualTo("60.543564");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book7");
				bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("3");
				assertThat(bs.getValue("sa").stringValue()).isEqualTo("3");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book8");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomAndStandardSum_WithFilter() {
		String query = "select (<" + functionFactory.getIri() + ">(distinct ?o) as ?m) (sum(?o) as ?sa) where { \n"
				+ "\t?s ?p ?o . filter(?o > 0) }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				BindingSet bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("418.2335645814707");
				assertThat(bs.getValue("sa").stringValue()).isEqualTo("462.0335626741221");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_InvalidValues() {
		String query = "select (<" + functionFactory.getIri() + ">(distinct ?o) as ?m) (sum(?o) as ?sa) where { \n"
				+ "\t?s ?p ?o .  }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				assertThat(result.next().size()).isEqualTo(0);
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_MultipleSumWithDistinctGroupBy() {
		String query = "select ?s (<" + functionFactory.getIri() + ">(distinct ?o) as ?m) where { \n"

				+ "\t?s <urn:n> ?o . filter(?o > 0) } group by ?s order by ?s";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				assertThat(result.next().getValue("m").stringValue()).isEqualTo("12.5");
				assertThat(result.next().getValue("m").stringValue()).isEqualTo("6");
				assertThat(result.next().getValue("m").stringValue()).isEqualTo("12.5");
				assertThat(result.next().getValue("m").stringValue()).isEqualTo("31.3");
				assertThat(result.next().getValue("m").stringValue()).isEqualTo("0.090000200001");
				assertThat(result.next().getValue("m").stringValue()).isEqualTo("60.543564");
				assertThat(result.next().getValue("m").stringValue()).isEqualTo("3");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_MultipleSumWithHaving() {
		String query = "select ?s (<" + functionFactory.getIri() + ">(?o) as ?m) where { \n"
				+ "\t?s <urn:n> ?o . }\n" +
				"GROUP BY ?s \n" +
				"HAVING((<" + functionFactory.getIri() + ">( ?o)) > 60)";
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery tupleQuery = conn.prepareTupleQuery(query);
			try (TupleQueryResult result = tupleQuery.evaluate()) {
				assertThat(result.next().getValue("s").stringValue()).isEqualTo("http://example/book7");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testSumWithHavingFilter() {
		String query = "select ?s (SUM(?o) as ?sum)  where { \n"
				+ "\t?s <urn:n> ?o . }\n" +
				"GROUP BY ?s \n" +
				"HAVING(SUM(?o) > 60)";
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery tupleQuery = conn.prepareTupleQuery(query);
			try (TupleQueryResult result = tupleQuery.evaluate()) {
				assertThat(result.next().getValue("s").stringValue()).isEqualTo("http://example/book7");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testAvgWithHavingFilter() {
		String query = "select ?s (avg(?o) as ?avg)  where { \n"
				+ "\t?s <urn:n> ?o . }\n" +
				"GROUP BY ?s \n" +
				"HAVING(avg(?o) > 30)";
		try (RepositoryConnection conn = rep.getConnection()) {
			TupleQuery tupleQuery = conn.prepareTupleQuery(query);
			try (TupleQueryResult result = tupleQuery.evaluate()) {
				var resultList = result.stream().collect(Collectors.toList());
				assertThat(resultList.size()).isEqualTo(2);
				assertThat(resultList.stream()
						.anyMatch(bs -> bs.getValue("s").stringValue().equals("http://example/book4"))).isTrue();
				assertThat(resultList.stream()
						.anyMatch(bs -> bs.getValue("s").stringValue().equals("http://example/book7"))).isTrue();
			}
		}
	}

	@Test
	public void testNonExistentCustomAggregateFunction() {
		String query = "select (<http://example.org/doesNotExist>(distinct ?o) as ?m) where { ?s <urn:n> ?o . }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				QueryEvaluationException queryEvaluationException = Assertions
						.assertThrows(QueryEvaluationException.class, result::next);
				Assertions.assertTrue(queryEvaluationException.toString().contains("aggregate"));
			}
		}
	}

	@Test
	public void testNonExistentCustomAggregateFunction2() {
		String query = "select (<http://example.org/doesNotExist>(?o) as ?m) where { ?s <urn:n> ?o . }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				QueryEvaluationException queryEvaluationException = Assertions
						.assertThrows(QueryEvaluationException.class, result::next);
				Assertions.assertFalse(queryEvaluationException.toString().contains("aggregate"));
			}
		}
	}

	@Test
	public void testNonExistentCustomAggregateFunction3() {
		String query = "select ?s (<http://example.org/doesNotExist>(?o) as ?m) where { ?s <urn:n> ?o . } group by ?s";
		try (RepositoryConnection conn = rep.getConnection()) {
			MalformedQueryException queryEvaluationException = Assertions.assertThrows(MalformedQueryException.class,
					() -> {
						TupleQuery tupleQuery = conn.prepareTupleQuery(query);
						try (TupleQueryResult result = tupleQuery.evaluate()) {
							result.next();
						}
					});
			Assertions.assertTrue(queryEvaluationException.toString().contains("non-aggregate"));

		}
	}

	private static void addData() throws IOException {
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(new StringReader(" <http://example/book1> <urn:n> \"12.5\"^^xsd:float .\n"
					+ "    <http://example/book1> <urn:n1> \"12.5\"^^xsd:float .\n"
					+ "    <http://example/book1> <urn:n1> \"12.6\"^^xsd:float .\n"
					+ "    <http://example/book2> <urn:n> \"6\"^^xsd:int .\n"
					+ "    <http://example/book3> <urn:n>  \"12.5\"^^xsd:double .\n"
					+ "    <http://example/book4> <urn:n>  31.3 .\n"
					+ "    <http://example/book5> <urn:n>  \"311.11241cawda3\" .\n"
					+ "    <http://example/book5> <urn:n1> 311 .\n"
					+ "    <http://example/book6> <urn:n>  0.090000200001 .\n"
					+ "    <http://example/book7> <urn:n>  31.3 .\n"
					+ "    <http://example/book7> <urn:n>  29.243564 .\n"
					+ "    <http://example/book8> <urn:n>  3 ."), "", RDFFormat.TURTLE);
		}
	}

	/**
	 * Dummy collector to verify custom aggregate functions
	 */
	private static class SumCollector implements ExtendedAggregateCollector {
		private final AtomicReference<ValueExprEvaluationException> typeErrorRef = new AtomicReference<>();

		private final AtomicReference<Literal> valueRef = new AtomicReference<>(SimpleValueFactory.getInstance().createLiteral("0", CoreDatatype.XSD.INTEGER));

		@Override
		public Value getFinalValue(TripleSource ts) {
			ValueExprEvaluationException ex = typeErrorRef.get();
			if (ex != null) {
				// a type error occurred while processing the aggregate, throw it now.
				throw ex;
			}
			return valueRef.get();
		}
	}



	@Test
	public void testCustomFunction_MaxWith() {
		String query = "prefix halyard: <"+HALYARD.NAMESPACE+"> \n"
				+ "select (<" + new MaxWithAggregateFactory().getIri() + ">(distinct halyard:tuple(?o,?p)) as ?m) "
				+ " ?s where { \n"
				+ "\t ?s ?p ?o . } group by ?s order by ?s limit 1";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				var bs = result.next();
				assertThat(((TupleLiteral)bs.getValue("m")).objectValue()[0].stringValue()).isEqualTo("12.6");
				assertThat(((TupleLiteral)bs.getValue("m")).objectValue()[1].stringValue()).isEqualTo("urn:n1");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book1");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_MinWith() {
		String query = "prefix halyard: <"+HALYARD.NAMESPACE+"> \n"
				+ "select (<" + new MinWithAggregateFactory().getIri() + ">(distinct halyard:tuple(?o,?p)) as ?m) "
				+ " ?s where { \n"
				+ "\t ?s ?p ?o . } group by ?s order by ?s limit 1";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				var bs = result.next();
				assertThat(((TupleLiteral)bs.getValue("m")).objectValue()[0].stringValue()).isEqualTo("12.5");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book1");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_Mode() {
		String query = "prefix halyard: <"+HALYARD.NAMESPACE+"> \n"
				+ "select (<" + new ModeAggregateFactory().getIri() + ">(?o) as ?m) "
				+ " ?s where { \n"
				+ "\t ?s ?p ?o . } group by ?s order by ?s limit 1";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				var bs = result.next();
				assertThat(bs.getValue("m").stringValue()).isEqualTo("12.5");
				assertThat(bs.getValue("s").stringValue()).isEqualTo("http://example/book1");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_TopNWith() {
		String query = "prefix halyard: <"+HALYARD.NAMESPACE+"> \n"
				+ "select (<" + new TopNWithAggregateFactory().getIri() + ">(halyard:tuple(?o,2,?s)) as ?m) "
				+ " where { \n"
				+ "\t ?s <urn:n1> ?o . }";
		try (RepositoryConnection conn = rep.getConnection()) {
			try (TupleQueryResult result = conn.prepareTupleQuery(query).evaluate()) {
				var bs = result.next();
				TupleLiteral top2 = (TupleLiteral) bs.getValue("m");
				TupleLiteral r1 = TupleLiteral.asTupleLiteral(top2.objectValue()[0]);
				TupleLiteral r2 = TupleLiteral.asTupleLiteral(top2.objectValue()[1]);
				assertThat(r1.objectValue()[0].stringValue()).isEqualTo("311");
				assertThat(r1.objectValue()[2].stringValue()).isEqualTo("http://example/book5");
				assertThat(r2.objectValue()[0].stringValue()).isEqualTo("12.6");
				assertThat(r2.objectValue()[2].stringValue()).isEqualTo("http://example/book1");
				assertThat(result.hasNext()).isFalse();
			}
		}
	}

	@Test
	public void testCustomFunction_HIndex() throws IOException {
		String countQuery = "select (count(distinct ?t) as ?count) {[] <x:name> ?t}";
		String testQuery = "prefix halyard: <"+HALYARD.NAMESPACE+"> \n"
				+ "select ?t (<" + new HIndexAggregateFactory().getIri() + ">(?c) as ?h) (sample(?e) as ?expected)"
				+ " where { \n"
				+ "\t [<x:cites> ?c; <x:expected> ?e; <x:name> ?t] }"
				+ " GROUP BY ?t";
		IRI graph = rep.getValueFactory().createIRI("<x:TestGraph>");
		try (RepositoryConnection conn = rep.getConnection()) {
			conn.add(new StringReader(""
				+ "[<x:cites> 1; <x:expected> 1; <x:name> 'test1'] ."
				+ "[<x:cites> 1; <x:expected> 1; <x:name> 'test2'] ."
				+ "[<x:cites> 1; <x:expected> 1; <x:name> 'test2'] ."
				+ "[<x:cites> 1; <x:expected> 1; <x:name> 'test2'] ."
				+ "[<x:cites> 9; <x:expected> 3; <x:name> 'test3'] ."
				+ "[<x:cites> 7; <x:expected> 3; <x:name> 'test3'] ."
				+ "[<x:cites> 6; <x:expected> 3; <x:name> 'test3'] ."
				+ "[<x:cites> 2; <x:expected> 3; <x:name> 'test3'] ."
				+ "[<x:cites> 1; <x:expected> 3; <x:name> 'test3'] ."
				+ "[<x:cites> 10; <x:expected> 4; <x:name> 'test4'] ."
				+ "[<x:cites> 8; <x:expected> 4; <x:name> 'test4'] ."
				+ "[<x:cites> 5; <x:expected> 4; <x:name> 'test4'] ."
				+ "[<x:cites> 4; <x:expected> 4; <x:name> 'test4'] ."
				+ "[<x:cites> 3; <x:expected> 4; <x:name> 'test4'] ."
				+ "[<x:cites> 25; <x:expected> 3; <x:name> 'test5'] ."
				+ "[<x:cites> 8; <x:expected> 3; <x:name> 'test5'] ."
				+ "[<x:cites> 5; <x:expected> 3; <x:name> 'test5'] ."
				+ "[<x:cites> 3; <x:expected> 3; <x:name> 'test5'] ."
				+ "[<x:cites> 3; <x:expected> 3; <x:name> 'test5'] ."
				+ "[<x:cites> 33; <x:expected> 6; <x:name> 'test6'] ."
				+ "[<x:cites> 30; <x:expected> 6; <x:name> 'test6'] ."
				+ "[<x:cites> 20; <x:expected> 6; <x:name> 'test6'] ."
				+ "[<x:cites> 15; <x:expected> 6; <x:name> 'test6'] ."
				+ "[<x:cites> 7; <x:expected> 6; <x:name> 'test6'] ."
				+ "[<x:cites> 6; <x:expected> 6; <x:name> 'test6'] ."
				+ "[<x:cites> 5; <x:expected> 6; <x:name> 'test6'] ."
				+ "[<x:cites> 4; <x:expected> 6; <x:name> 'test6'] ."
				+ ""), "", RDFFormat.TURTLE, graph);
			int expectedNumTests = 6;
			int numTests;
			try (TupleQueryResult result = conn.prepareTupleQuery(countQuery).evaluate()) {
				var bs = result.next();
				numTests = Literals.getIntValue(bs.getValue("count"), 0);
				assertThat(numTests).isEqualTo(expectedNumTests);
			}
			try (TupleQueryResult result = conn.prepareTupleQuery(testQuery).evaluate()) {
				for (int i=0; i<numTests; i++) {
					var bs = result.next();
					Literal test = (Literal) bs.getValue("t");
					Literal h = (Literal) bs.getValue("h");
					Literal expected = (Literal) bs.getValue("expected");
					assertThat(h).as(test.getLabel()).isEqualTo(expected);
				}
			}
			conn.clear(graph);
		}
	}
}
