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
package com.msd.gin.halyard.sail;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TripleRef;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardStatsBasedStatementPatternCardinalityCalculator extends SimpleStatementPatternCardinalityCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(HalyardStatsBasedStatementPatternCardinalityCalculator.class);

	private final TripleSource statsSource;
	private final RDFFactory rdfFactory;
	private final Cache<IRI, Long> stmtCountCache;

	static Cache<IRI, Long> newStatementCountCache() {
		// NB: use concurrency of 1 else waste lots of HBase connections retrieving the same data concurrently
		return CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(1000).build();
	}

	public HalyardStatsBasedStatementPatternCardinalityCalculator(TripleSource statsSource, RDFFactory rdfFactory, Cache<IRI, Long> stmtCountCache) {
		this.statsSource = statsSource;
		this.rdfFactory = rdfFactory;
		this.stmtCountCache = stmtCountCache;
	}

	@Override
	public double getCardinality(StatementPattern sp, Collection<String> boundVars) {
		IRI graphNode;
		Var contextVar = sp.getContextVar();
		Value contextValue = (contextVar != null) ? contextVar.getValue() : null;
		if (contextValue == null) {
			graphNode = HALYARD.STATS_ROOT_NODE;
		} else {
			graphNode = contextValue.isIRI() ? (IRI) contextValue : null;
		}
		Double card = (graphNode != null) ? getCardinalityFromStats(sp.getSubjectVar(), sp.getPredicateVar(), sp.getObjectVar(), graphNode, boundVars) : null;
		if (card != null) {
			LOG.debug("Cardinality of {} = {} (sampled)", sp, card);
		} else {
			card = super.getCardinality(sp, boundVars);
			LOG.debug("Cardinality of {} = {} (preset)", sp, card);
		}
		return card;
	}

	@Override
	public double getCardinality(TripleRef tripleRef, Collection<String> boundVars) {
		Double card = getCardinalityFromStats(tripleRef.getSubjectVar(), tripleRef.getPredicateVar(), tripleRef.getObjectVar(), HALYARD.TRIPLE_GRAPH_CONTEXT, boundVars);
		if (card != null) {
			LOG.debug("Cardinality of {} = {} (sampled)", tripleRef, card);
		} else {
			card = super.getCardinality(tripleRef, boundVars);
			LOG.debug("Cardinality of {} = {} (preset)", tripleRef, card);
		}
		return card;
	}

	/**
	 * Get the cardinality from VOID statistics.
	 */
	private Double getCardinalityFromStats(Var subjVar, Var predVar, Var objVar, IRI graphNode, Collection<String> boundVars) {
		final long triples = getTriplesCount(graphNode, -1l);
		if (triples == -1l) {
			return null;
		}

		double card;
		boolean sv = hasValue(subjVar, boundVars);
		boolean pv = hasValue(predVar, boundVars);
		boolean ov = hasValue(objVar, boundVars);
		long defaultCardinality = Math.round(Math.sqrt(triples));
		if (sv) {
			if (pv) {
				if (ov) {
					card = 1.0;
				} else {
					card = subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, subjVar, defaultCardinality) * subsetTriplesPart(graphNode, VOID.PROPERTY, predVar, defaultCardinality) / triples;
				}
			} else if (ov) {
				card = subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, subjVar, defaultCardinality) * subsetTriplesPart(graphNode, VOID_EXT.OBJECT, objVar, defaultCardinality) / triples;
			} else {
				card = subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, subjVar, defaultCardinality);
			}
		} else if (pv) {
			if (ov) {
				card = subsetTriplesPart(graphNode, VOID.PROPERTY, predVar, defaultCardinality) * subsetTriplesPart(graphNode, VOID_EXT.OBJECT, objVar, defaultCardinality) / triples;
			} else {
				card = subsetTriplesPart(graphNode, VOID.PROPERTY, predVar, defaultCardinality);
			}
		} else if (ov) {
			card = subsetTriplesPart(graphNode, VOID_EXT.OBJECT, objVar, defaultCardinality);
		} else {
			card = triples;
		}
		return card;
	}

	/**
	 * Get the triple count for a given subject from VOID statistics or return the default value.
	 */
	private long getTriplesCount(IRI subjectNode, long defaultValue) {
		try {
			return stmtCountCache.get(subjectNode, () -> {
				try (CloseableIteration<? extends Statement, QueryEvaluationException> ci = statsSource.getStatements(subjectNode, VOID.TRIPLES, null, HALYARD.STATS_GRAPH_CONTEXT)) {
					if (ci.hasNext()) {
						Value v = ci.next().getObject();
						if (v.isLiteral()) {
							try {
								long l = ((Literal) v).longValue();
								LOG.trace("Triple stats for {} = {}", subjectNode, l);
								return l;
							} catch (NumberFormatException ignore) {
								LOG.warn("Invalid statistics for {}: {}", subjectNode, v, ignore);
							}
						}
						LOG.warn("Invalid statistics for {}: {}", subjectNode, v);
					}
				}
				LOG.trace("Triple stats for {} are not available", subjectNode);
				return defaultValue;
			});
		} catch (ExecutionException ee) {
			LOG.warn("Error retrieving statistics for {}", subjectNode, ee.getCause());
			return defaultValue;
		}
	}

	/**
	 * Calculate a multiplier for the triple count for this sub-part of the graph.
	 */
	private double subsetTriplesPart(IRI graph, IRI partitionType, Var partitionVar, long defaultCardinality) {
		if (partitionVar == null || !partitionVar.hasValue()) {
			return defaultCardinality;
		}
		IRI partitionIri = createPartitionIRI(graph, partitionType, partitionVar.getValue(), rdfFactory, statsSource.getValueFactory());
		return getTriplesCount(partitionIri, defaultCardinality);
	}

	@Override
	public void close() throws IOException {
		if (statsSource instanceof Closeable) {
			((Closeable) statsSource).close();
		}
	}

	public static IRI createPartitionIRI(IRI graph, IRI partitionType, Value partitionId, RDFFactory rdfFactory, ValueFactory vf) {
		return vf.createIRI(graph.stringValue() + "_" + partitionType.getLocalName() + "_" + rdfFactory.id(partitionId).toString());
	}
}
