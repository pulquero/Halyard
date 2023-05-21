package com.msd.gin.halyard.sail;

import com.github.benmanes.caffeine.cache.Cache;
import com.msd.gin.halyard.model.ModelTripleSource;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class HalyardStatsBasedStatementPatternCardinalityCalculatorTest {
	private final ValueFactory vf = SimpleValueFactory.getInstance();
	private final IRI graph1 = vf.createIRI("http://graph1");
	private final IRI subj = vf.createIRI("http://subject");
	private final IRI pred = vf.createIRI("http://predicate");
	private final IRI obj = vf.createIRI("http://object");
	private HalyardStatsBasedStatementPatternCardinalityCalculator calc;

	@BeforeEach
	public void setup() {
		IRI graphNode = HALYARD.STATS_ROOT_NODE;
		HalyardStatsBasedStatementPatternCardinalityCalculator.PartitionIriTransformer transformer = (graph, partitionType, partitionId) -> graph.stringValue() + "_" + partitionType.getLocalName() + "_" + partitionId.stringValue();
		Model model = new LinkedHashModel();
		model.add(graphNode, VOID.TRIPLES, vf.createLiteral(13), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(graphNode, VOID.DISTINCT_SUBJECTS, vf.createLiteral(3), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(graphNode, VOID.PROPERTIES, vf.createLiteral(5), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(vf.createIRI(transformer.apply(graphNode, VOID_EXT.SUBJECT, subj)), VOID.TRIPLES, vf.createLiteral(6), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(vf.createIRI(transformer.apply(graphNode, VOID.PROPERTY, pred)), VOID.TRIPLES, vf.createLiteral(4), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(vf.createIRI(transformer.apply(graphNode, VOID_EXT.OBJECT, obj)), VOID.DISTINCT_SUBJECTS, vf.createLiteral(2), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(vf.createIRI(transformer.apply(graphNode, VOID_EXT.OBJECT, obj)), VOID.TRIPLES, vf.createLiteral(3), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(graph1, VOID.TRIPLES, vf.createLiteral(25), HALYARD.STATS_GRAPH_CONTEXT);
		ModelTripleSource ts = new ModelTripleSource(model, vf);
		Cache<Pair<IRI, IRI>, Long> cache = HalyardStatsBasedStatementPatternCardinalityCalculator.newStatisticsCache();
		calc = new HalyardStatsBasedStatementPatternCardinalityCalculator(ts, transformer, cache);
	}

	@Test
	public void testSingleValue() {
		double card = calc.getStatementCardinality(new Var("s", subj), new Var("p"), new Var("o"), null, Collections.emptySet());
		assertEquals(6.0, card);
	}

	@Test
	public void testDoubleValue() {
		double card = calc.getStatementCardinality(new Var("s", subj), new Var("p", pred), new Var("o"), null, Collections.emptySet());
		assertEquals(6.0 * 4.0 / 13.0, card);
	}

	@Test
	public void testDoubleValueUsingRatio() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o", obj), null, boundVars);
		assertEquals((13.0 / 3.0) * (3.0 / 2.0), card);
	}

	@Test
	public void testSingleBoundVar() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), null, boundVars);
		assertEquals(13.0 / 3.0, card);
	}

	@Test
	public void testDoubleBoundVar() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		boundVars.add("p");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), null, boundVars);
		assertEquals((13.0 / 3.0 * 13.0 / 5.0) / 13.0, card);
	}

	@Test
	public void testSingleBoundVarNoPartitionStats() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), new Var("c", vf.createIRI("http://graph1")), boundVars);
		assertEquals(Math.sqrt(25), card);
	}

	@Test
	public void testDoubleBoundVarNoPartitionStats() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		boundVars.add("p");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), new Var("c", vf.createIRI("http://graph1")), boundVars);
		assertEquals(1.0, card);
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
