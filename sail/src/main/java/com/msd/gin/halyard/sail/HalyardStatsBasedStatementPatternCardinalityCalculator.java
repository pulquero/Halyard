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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.msd.gin.halyard.algebra.evaluation.CloseableTripleSource;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.vocab.HALYARD;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardStatsBasedStatementPatternCardinalityCalculator extends SimpleStatementPatternCardinalityCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(HalyardStatsBasedStatementPatternCardinalityCalculator.class);
	private static final Map<IRI, IRI> DISTINCT_PREDICATES = createDistinctPredicateMapping();

	private static Map<IRI, IRI> createDistinctPredicateMapping() {
		Map<IRI, IRI> mapping = new HashMap<>();
		mapping.put(VOID_EXT.SUBJECT, VOID.DISTINCT_SUBJECTS);
		mapping.put(VOID.PROPERTY, VOID.PROPERTIES);
		mapping.put(VOID_EXT.OBJECT, VOID.DISTINCT_OBJECTS);
		return Collections.unmodifiableMap(mapping);
	}

	public static interface PartitionIriTransformer {
		String apply(IRI graph, IRI partitionType, Value partitionId);
	}

	public static PartitionIriTransformer createPartitionIriTransformer(RDFFactory rdfFactory) {
		return (graph, partitionType, partitionId) -> graph.stringValue() + "_" + partitionType.getLocalName() + "_" + rdfFactory.id(partitionId).toString();
	}

	private final CloseableTripleSource statsSource;
	private final PartitionIriTransformer partitionIriTransformer;
	private final Cache<Pair<IRI, IRI>, Long> stmtCountCache;

	static Cache<Pair<IRI, IRI>, Long> newStatisticsCache() {
		return Caffeine.newBuilder().maximumSize(100).expireAfterWrite(1L, TimeUnit.DAYS).build();
	}

	public HalyardStatsBasedStatementPatternCardinalityCalculator(CloseableTripleSource statsSource, RDFFactory rdfFactory, Cache<Pair<IRI, IRI>, Long> stmtCountCache) {
		this(statsSource, createPartitionIriTransformer(rdfFactory), stmtCountCache);
	}

	public HalyardStatsBasedStatementPatternCardinalityCalculator(CloseableTripleSource statsSource, PartitionIriTransformer partitionIriTransformer, Cache<Pair<IRI, IRI>, Long> stmtCountCache) {
		this.statsSource = statsSource;
		this.partitionIriTransformer = partitionIriTransformer;
		this.stmtCountCache = stmtCountCache;
	}

	@Override
	public double getStatementCardinality(Var subjVar, Var predVar, Var objVar, Var ctxVar, Collection<String> boundVars) {
		IRI graphNode;
		Value contextValue = (ctxVar != null) ? ctxVar.getValue() : null;
		if (contextValue == null) {
			graphNode = HALYARD.STATS_ROOT_NODE;
		} else {
			graphNode = contextValue.isIRI() ? (IRI) contextValue : null;
		}
		Double card = (graphNode != null) ? getCardinalityFromStats(subjVar, predVar, objVar, graphNode, boundVars) : null;
		if (card != null) {
			LOG.debug("Cardinality of statement {} {} {} {} = {} (sampled)", subjVar, predVar, objVar, ctxVar, card);
		} else {
			card = super.getStatementCardinality(subjVar, predVar, objVar, ctxVar, boundVars);
			LOG.debug("Cardinality of statement {} {} {} {} = {} (preset)", subjVar, predVar, objVar, ctxVar, card);
		}
		return card;
	}

	@Override
	public double getTripleCardinality(Var subjVar, Var predVar, Var objVar, Collection<String> boundVars) {
		Double card = getCardinalityFromStats(subjVar, predVar, objVar, HALYARD.TRIPLE_GRAPH_CONTEXT, boundVars);
		if (card != null) {
			LOG.debug("Cardinality of triple {} {} {} = {} (sampled)", subjVar, predVar, objVar, card);
		} else {
			card = super.getTripleCardinality(subjVar, predVar, objVar, boundVars);
			LOG.debug("Cardinality of triple {} {} {} = {} (preset)", subjVar, predVar, objVar, card);
		}
		return card;
	}

	/**
	 * Get the cardinality from VOID statistics.
	 */
	private Double getCardinalityFromStats(Var subjVar, Var predVar, Var objVar, IRI graphNode, Collection<String> boundVars) {
		final long triples = getTriplesCount(graphNode, -1L);
		if (triples == -1L) {
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
					card = subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, subjVar, triples, defaultCardinality) * subsetTriplesPart(graphNode, VOID.PROPERTY, predVar, triples, defaultCardinality) / triples;
				}
			} else if (ov) {
				card = subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, subjVar, triples, defaultCardinality) * subsetTriplesPart(graphNode, VOID_EXT.OBJECT, objVar, triples, defaultCardinality) / triples;
			} else {
				card = subsetTriplesPart(graphNode, VOID_EXT.SUBJECT, subjVar, triples, defaultCardinality);
			}
		} else if (pv) {
			if (ov) {
				card = subsetTriplesPart(graphNode, VOID.PROPERTY, predVar, triples, defaultCardinality) * subsetTriplesPart(graphNode, VOID_EXT.OBJECT, objVar, triples, defaultCardinality) / triples;
			} else {
				card = subsetTriplesPart(graphNode, VOID.PROPERTY, predVar, triples, defaultCardinality);
			}
		} else if (ov) {
			card = subsetTriplesPart(graphNode, VOID_EXT.OBJECT, objVar, triples, defaultCardinality);
		} else {
			card = triples;
		}
		return card;
	}

	/**
	 * Get the triple count for a given subject from VOID statistics or return the default value.
	 */
	private long getTriplesCount(IRI subjectNode, long defaultValue) {
		return getCount(subjectNode, VOID.TRIPLES, defaultValue);
	}

	private long getCount(IRI subjectNode, IRI countPredicate, long defaultValue) {
		try {
			return stmtCountCache.get(Pair.of(subjectNode, countPredicate), sp -> {
				IRI statsNode = sp.getLeft();
				IRI statsPred = sp.getRight();
				try (CloseableIteration<? extends Statement, QueryEvaluationException> ci = statsSource.getStatements(statsNode, statsPred, null, HALYARD.STATS_GRAPH_CONTEXT)) {
					if (ci.hasNext()) {
						Value v = ci.next().getObject();
						if (v.isLiteral()) {
							try {
								long l = ((Literal) v).longValue();
								LOG.trace("{} statistics for {} = {}", statsPred, statsNode, l);
								return l;
							} catch (NumberFormatException ignore) {
								LOG.warn("Invalid {} statistics for {}: {}", statsPred, statsNode, v, ignore);
							}
						}
						LOG.warn("Invalid {} statistics for {}: {}", statsPred, statsNode, v);
					}
				}
				LOG.trace("{} statistics for {} are not available", statsPred, statsNode);
				return defaultValue;
			});
		} catch (Exception e) {
			LOG.warn("Error retrieving {} statistics for {}", countPredicate, subjectNode, e);
			return defaultValue;
		}
	}

	/**
	 * Calculate a multiplier for the triple count for this sub-part of the graph.
	 */
	private double subsetTriplesPart(IRI graph, IRI partitionType, Var partitionVar, long totalTriples, long defaultCardinality) {
		if (partitionVar.hasValue()) {
			IRI partitionIri = statsSource.getValueFactory().createIRI(partitionIriTransformer.apply(graph, partitionType, partitionVar.getValue()));
			return getTriplesCount(partitionIri, defaultCardinality);
		} else {
			long distinctCount = getCount(graph, DISTINCT_PREDICATES.get(partitionType), -1L);
			if (distinctCount != -1L) {
				// average cardinality for partitionType
				return (double) totalTriples / (double) distinctCount;
			} else {
				return defaultCardinality;
			}
		}
	}

	@Override
	public void close() throws IOException {
		statsSource.close();
	}
}
