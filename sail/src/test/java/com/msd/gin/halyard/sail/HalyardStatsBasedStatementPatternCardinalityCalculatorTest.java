package com.msd.gin.halyard.sail;

import com.github.benmanes.caffeine.cache.Cache;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.query.algebra.evaluation.ModelTripleSource;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HalyardStatsBasedStatementPatternCardinalityCalculatorTest {
	private static final int TOTAL_TRIPLES = 13;
	private static final int TRIPLES_WITH_SUBJ = 6;
	private static final int TRIPLES_WITH_PRED = 4;
	private static final int TRIPLES_WITH_OBJ = 3;
	private static final int GRAPH_TRIPLES = 25;

	private final ValueFactory vf = SimpleValueFactory.getInstance();
	private final IRI graph1 = vf.createIRI("http://graph1");
	private final IRI subj = vf.createIRI("http://subject");
	private final IRI pred = vf.createIRI("http://predicate");
	private final IRI obj = vf.createIRI("http://object");
	private HalyardStatsBasedStatementPatternCardinalityCalculator.PartitionIriTransformer transformer;
	private HalyardStatsBasedStatementPatternCardinalityCalculator calc;

	@BeforeEach
	public void setup() {
		IRI graphNode = HALYARD.STATS_ROOT_NODE;
		transformer = new HalyardStatsBasedStatementPatternCardinalityCalculator.PartitionIriTransformer() {
			protected String id(Value partitionId) {
				return partitionId.stringValue();
			}
		};
		Model model = new LinkedHashModel();
		model.add(graphNode, VOID.TRIPLES, vf.createLiteral(TOTAL_TRIPLES), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(graphNode, VOID.DISTINCT_SUBJECTS, vf.createLiteral(3), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(graphNode, VOID.PROPERTIES, vf.createLiteral(5), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(vf.createIRI(transformer.apply(graphNode, VOID_EXT.SUBJECT, subj)), VOID.TRIPLES, vf.createLiteral(TRIPLES_WITH_SUBJ), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(vf.createIRI(transformer.apply(graphNode, VOID.PROPERTY, pred)), VOID.TRIPLES, vf.createLiteral(TRIPLES_WITH_PRED), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(vf.createIRI(transformer.apply(graphNode, VOID_EXT.OBJECT, obj)), VOID.DISTINCT_SUBJECTS, vf.createLiteral(2), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(vf.createIRI(transformer.apply(graphNode, VOID_EXT.OBJECT, obj)), VOID.TRIPLES, vf.createLiteral(TRIPLES_WITH_OBJ), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(graph1, VOID.TRIPLES, vf.createLiteral(GRAPH_TRIPLES), HALYARD.STATS_GRAPH_CONTEXT);
		ModelTripleSource ts = new ModelTripleSource(model, vf);
		Cache<Pair<IRI, IRI>, Long> cache = HalyardStatsBasedStatementPatternCardinalityCalculator.newStatisticsCache();
		calc = new HalyardStatsBasedStatementPatternCardinalityCalculator(ts, transformer, cache);
	}

	@Test
	public void testPartitionIriTransformer() {
		IRI graph = vf.createIRI("http://graph");
		Value partitionId = vf.createLiteral(0);
		for (IRI partitionType : Arrays.asList(VOID_EXT.SUBJECT, VOID.PROPERTY, VOID_EXT.OBJECT)) {
			String graphPart = transformer.getGraph(vf.createIRI(transformer.apply(graph, partitionType, partitionId)));
			assertEquals(graph.stringValue(), graphPart);
		}
	}

	@Test
	public void testSingleValue() {
		double card = calc.getStatementCardinality(new Var("s", subj), new Var("p"), new Var("o"), null, Collections.emptySet());
		assertEquals(TRIPLES_WITH_SUBJ, card);
	}

	@Test
	public void testDoubleValue() {
		double card = calc.getStatementCardinality(new Var("s", subj), new Var("p", pred), new Var("o"), null, Collections.emptySet());
		assertThat(card).isLessThan(TRIPLES_WITH_SUBJ);
		assertThat(card).isLessThan(TRIPLES_WITH_PRED);
		assertEquals(3.0, card);
	}

	@Test
	public void testDoubleValueUsingRatio() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o", obj), null, boundVars);
		assertThat(card).isLessThan(TRIPLES_WITH_OBJ);
		assertEquals(2.0, card);
	}

	@Test
	public void testSingleBoundVar() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), null, boundVars);
		assertThat(card).isLessThan(TOTAL_TRIPLES);
		assertEquals(Math.ceil(13.0 / 3.0), card);
	}

	@Test
	public void testDoubleBoundVar() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		boundVars.add("p");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), null, boundVars);
		assertThat(card).isLessThan(TOTAL_TRIPLES);
		assertEquals(2.0, card);
	}

	@Test
	public void testSingleBoundVarNoPartitionStats() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), new Var("c", vf.createIRI("http://graph1")), boundVars);
		assertThat(card).isLessThan(GRAPH_TRIPLES);
		assertEquals(Math.sqrt(25.0), card);
	}

	@Test
	public void testDoubleBoundVarNoPartitionStats() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		boundVars.add("p");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), new Var("c", vf.createIRI("http://graph1")), boundVars);
		assertThat(card).isLessThan(GRAPH_TRIPLES);
		assertEquals(3.0, card);
	}

	@Test
	public void testSingleBoundVarNoStats() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), new Var("c", vf.createIRI("http://no-stats")), boundVars);
		assertEquals(SimpleStatementPatternCardinalityCalculator.PREDICATE_VAR_CARDINALITY * SimpleStatementPatternCardinalityCalculator.OBJECT_VAR_CARDINALITY, card);
	}

	@Test
	public void testDoubleBoundVarNoStats() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		boundVars.add("p");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), new Var("c", vf.createIRI("http://no-stats")), boundVars);
		assertEquals(SimpleStatementPatternCardinalityCalculator.OBJECT_VAR_CARDINALITY, card);
	}
}
