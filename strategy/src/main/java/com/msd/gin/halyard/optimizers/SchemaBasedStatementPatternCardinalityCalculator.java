package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.vocab.VOID_EXT;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.vocabulary.WGS84;
import org.eclipse.rdf4j.query.algebra.Var;

public class SchemaBasedStatementPatternCardinalityCalculator extends SimpleStatementPatternCardinalityCalculator {
	private static final Map<IRI, Integer> SCHEMA_CARDINALITIES = createSchemaCardinalities();

	private static Map<IRI, Integer> createSchemaCardinalities() {
		Map<IRI, Integer> cards = new HashMap<>(32);
		cards.put(SD.DEFAULT_DATASET, 1);
		cards.put(SD.DEFAULT_GRAPH, 1);
		cards.put(SD.NAME, 1);
		cards.put(VOID.CLASS, 1);
		cards.put(VOID.PROPERTY, 1);
		cards.put(VOID.CLASSES, 1);
		cards.put(VOID.ENTITIES, 1);
		cards.put(VOID.TRIPLES, 1);
		cards.put(VOID.PROPERTIES, 1);
		cards.put(VOID.DISTINCT_SUBJECTS, 1);
		cards.put(VOID.DISTINCT_OBJECTS, 1);
		cards.put(VOID_EXT.SUBJECT, 1);
		cards.put(VOID_EXT.OBJECT, 1);
		cards.put(WGS84.LAT, 1);
		cards.put(WGS84.LONG, 1);
		cards.put(WGS84.LAT_LONG, 1);
		cards.put(WGS84.ALT, 1);
		return Collections.unmodifiableMap(cards);
	}

	@Override
	public double getStatementCardinality(Var subjVar, Var predVar, Var objVar, Var ctxVar, Collection<String> boundVars) {
		Value pred = predVar.getValue();
		int schemaCardinality = (pred != null) ? getSchemaPredicateCardinality(pred) : -1;
		boolean ov = hasValue(objVar, boundVars);
		if (schemaCardinality != -1 && !ov) {
			return getSubjectCardinality(subjVar, boundVars) * schemaCardinality;
		} else {
			return super.getTripleCardinality(subjVar, predVar, objVar, boundVars);
		}
	}

	protected final int getSchemaPredicateCardinality(Value pred) {
		return SCHEMA_CARDINALITIES.getOrDefault(pred, -1);
	}
}
