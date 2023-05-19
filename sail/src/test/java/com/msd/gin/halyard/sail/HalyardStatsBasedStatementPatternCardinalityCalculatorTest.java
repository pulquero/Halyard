package com.msd.gin.halyard.sail;

import com.github.benmanes.caffeine.cache.Cache;
import com.msd.gin.halyard.model.ModelTripleSource;
import com.msd.gin.halyard.vocab.HALYARD;

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
	private HalyardStatsBasedStatementPatternCardinalityCalculator calc;

	@BeforeEach
	public void setup() {
		IRI graphNode = HALYARD.STATS_ROOT_NODE;
		ValueFactory vf = SimpleValueFactory.getInstance();
		Model model = new LinkedHashModel();
		model.add(graphNode, VOID.TRIPLES, vf.createLiteral(13), HALYARD.STATS_GRAPH_CONTEXT);
		model.add(graphNode, VOID.DISTINCT_SUBJECTS, vf.createLiteral(3), HALYARD.STATS_GRAPH_CONTEXT);
		ModelTripleSource ts = new ModelTripleSource(model, vf);
		Cache<Pair<IRI, IRI>, Long> cache = HalyardStatsBasedStatementPatternCardinalityCalculator.newStatisticsCache();
		HalyardStatsBasedStatementPatternCardinalityCalculator.PartitionIriTransformer transformer = (graph, partitionType, partitionId) -> graph.stringValue() + "_" + partitionType.getLocalName() + "_" + partitionId.stringValue();
		calc = new HalyardStatsBasedStatementPatternCardinalityCalculator(ts, transformer, cache);
	}

	@Test
	public void testBoundVars() {
		Set<String> boundVars = new HashSet<>();
		boundVars.add("s");
		double card = calc.getStatementCardinality(new Var("s"), new Var("p"), new Var("o"), null, boundVars);
		assertEquals(13.0 / 3.0, card);
	}
}
