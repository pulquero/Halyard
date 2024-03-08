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
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.optimizers.SchemaBasedStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.query.algebra.evaluation.CloseableTripleSource;
import com.msd.gin.halyard.vocab.VOID_EXT;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class HalyardStatsBasedStatementPatternCardinalityCalculator extends SchemaBasedStatementPatternCardinalityCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(HalyardStatsBasedStatementPatternCardinalityCalculator.class);
	private static final long AVERAGING_LIMIT = 100;
	private static final Map<IRI, IRI> DISTINCT_PREDICATES = createDistinctPredicateMapping();
	private static final Map<IRI, IRI> PARTITION_PREDICATES = createPartitionPredicateMapping();
	private static final Map<IRI, IRI> PARTITION_THRESHOLD_PREDICATES = createPartitionThresholdPredicateMapping();

	private static Map<IRI, IRI> createDistinctPredicateMapping() {
		Map<IRI, IRI> mapping = new HashMap<>();
		mapping.put(VOID_EXT.SUBJECT, VOID.DISTINCT_SUBJECTS);
		mapping.put(VOID.PROPERTY, VOID.PROPERTIES);
		mapping.put(VOID_EXT.OBJECT, VOID.DISTINCT_OBJECTS);
		return Collections.unmodifiableMap(mapping);
	}

	public static Map<IRI, IRI> createPartitionPredicateMapping() {
		Map<IRI, IRI> mapping = new HashMap<>();
		mapping.put(VOID_EXT.SUBJECT, VOID_EXT.SUBJECT_PARTITION);
		mapping.put(VOID.PROPERTY, VOID.PROPERTY_PARTITION);
		mapping.put(VOID_EXT.OBJECT, VOID_EXT.OBJECT_PARTITION);
		mapping.put(VOID.CLASS, VOID.CLASS_PARTITION);
		return Collections.unmodifiableMap(mapping);
	}

	private static Map<IRI, IRI> createPartitionThresholdPredicateMapping() {
		Map<IRI, IRI> mapping = new HashMap<>();
		mapping.put(VOID_EXT.SUBJECT, VOID_EXT.SUBJECT_PARTITION_THRESHOLD);
		mapping.put(VOID.PROPERTY, VOID_EXT.PROPERTY_PARTITION_THRESHOLD);
		mapping.put(VOID_EXT.OBJECT, VOID_EXT.OBJECT_PARTITION_THRESHOLD);
		mapping.put(VOID.CLASS, VOID_EXT.CLASS_PARTITION_THRESHOLD);
		return Collections.unmodifiableMap(mapping);
	}

	public static abstract class PartitionIriTransformer {
		public final String apply(IRI graph, IRI partitionType, Value partitionId) {
			return HALYARD.DATASET_NS + graph.stringValue() + "," + partitionType.getLocalName() + "," + id(partitionId);
		}

		protected abstract String id(Value partitionId);

		public final boolean isPartitionIri(String iri) {
			return iri.startsWith(HALYARD.DATASET_NS);
		}

		public final String getGraph(Resource partitionIri) {
			String partitionString = partitionIri.stringValue();
			if (!isPartitionIri(partitionString)) {
				return null;
			}
			int idSepPos = partitionString.lastIndexOf(",");
			if (idSepPos != -1) {
				int ptSepPos = partitionString.lastIndexOf(",", idSepPos - 1);
				if (ptSepPos != -1) {
					return partitionString.substring(HALYARD.DATASET_NS.length(), ptSepPos);
				}
			}
			return null;
		}
	}

	public static PartitionIriTransformer createPartitionIriTransformer(RDFFactory rdfFactory) {
		return new PartitionIriTransformer() {
			protected String id(Value partitionId) {
				return rdfFactory.id(partitionId).toString();
			}
		};
	}

	static Cache<Pair<IRI, IRI>, Long> newStatisticsCache() {
		return Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(1L, TimeUnit.DAYS).build();
	}

	private final CloseableTripleSource statsSource;
	private final PartitionIriTransformer partitionIriTransformer;
	private final Cache<Pair<IRI, IRI>, Long> stmtCountCache;

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
		Value subj = subjVar.getValue();
		Value pred = predVar.getValue();
		Value obj = objVar.getValue();
		long defaultCardinality = Math.round(Math.sqrt(triples));
		if (sv) {
			if (pv) {
				if (ov) {
					card = 1.0;
				} else {
					int schemaCardinality = (pred != null) ? getSchemaPredicateCardinality(pred) : -1;
					if (schemaCardinality != -1) {
						card = schemaCardinality;
					} else {
						card = subsetTriples(graphNode, VOID_EXT.SUBJECT, subj, VOID.PROPERTIES, VOID.PROPERTY, pred, VOID.DISTINCT_OBJECTS, triples, defaultCardinality);
					}
				}
			} else if (ov) {
				card = subsetTriples(graphNode, VOID_EXT.OBJECT, obj, VOID.DISTINCT_SUBJECTS, VOID_EXT.SUBJECT, subj, VOID.PROPERTIES, triples, defaultCardinality);
			} else {
				card = subsetTriples(graphNode, VOID_EXT.SUBJECT, subj, triples, defaultCardinality);
			}
		} else if (pv) {
			if (ov) {
				if (RDF.TYPE.equals(pred) && obj != null) {
					card = classPartitionEntities(graphNode, obj, defaultCardinality);
				} else {
					card = subsetTriples(graphNode, VOID.PROPERTY, pred, VOID.DISTINCT_OBJECTS, VOID_EXT.OBJECT, obj, VOID.DISTINCT_SUBJECTS, triples, defaultCardinality);
				}
			} else {
				card = subsetTriples(graphNode, VOID.PROPERTY, pred, triples, defaultCardinality);
			}
		} else if (ov) {
			card = subsetTriples(graphNode, VOID_EXT.OBJECT, obj, triples, defaultCardinality);
		} else {
			card = triples;
		}
		// round-up to a whole number
		return Math.ceil(card);
	}

	/**
	 * Get the triple count for a given subject from VOID statistics or return the default value.
	 */
	private long getTriplesCount(IRI subjectNode, long defaultValue) {
		return getValue(subjectNode, VOID.TRIPLES, defaultValue);
	}

	private long getValue(IRI subjectNode, IRI countPredicate, long defaultValue) {
		try {
			Long count = stmtCountCache.get(Pair.of(subjectNode, countPredicate), subjPred -> {
				IRI statsNode = subjPred.getLeft();
				IRI statsPred = subjPred.getRight();
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
				return null;
			});
			return (count != null) ? count.longValue() : defaultValue;
		} catch (Exception e) {
			LOG.warn("Error retrieving {} statistics for {}", countPredicate, subjectNode, e);
			return defaultValue;
		}
	}

	/**
	 * How many triples are there with one role known?
	 */
	private double subsetTriples(IRI graph, IRI partitionType, @Nullable Value partition, long totalTriples, long defaultCardinality) {
		if (partition != null) {
			// if we know the value then we can retrieve the triple count directly from stored stats

			// total number of triples of type 'partitionType' with the value 'partition'
			IRI partitionIri = statsSource.getValueFactory().createIRI(partitionIriTransformer.apply(graph, partitionType, partition));
			return getTriplesCount(partitionIri, defaultCardinality);
		} else {
			// if we don't know the value then we can use the average cardinality for that role
			return getCardinality(graph, partitionType, totalTriples, defaultCardinality);
		}
	}

	private double getCardinality(IRI graph, IRI partitionType, long totalTriples, long defaultCardinality) {
		// total number of distinct values
		long distinctCount = getValue(graph, DISTINCT_PREDICATES.get(partitionType), -1L);

		if (distinctCount != -1L) {
			double estimate = 0.0;
			if (distinctCount <= AVERAGING_LIMIT) {
				// few enough distinct values that we can calculate a weighted average cardinality
				// assume the bigger the partition, the more likely it is to be used:
				// sum_v x_v^2 / sum x_v, where x_v is the number of triples with value v
				long sumxx = 0L;
				long sumx = 0L;
				// get the partitions for all the values
				try (CloseableIteration<? extends Statement, QueryEvaluationException> iter = statsSource.getStatements(graph, PARTITION_PREDICATES.get(partitionType), null, HALYARD.STATS_GRAPH_CONTEXT)) {
					while (iter.hasNext()) {
						Statement stmt = iter.next();
						// get the number of triples for this value
						long count = getTriplesCount((IRI) stmt.getObject(), defaultCardinality);
						sumxx += count * count;
						sumx += count;
					}
				}
				long threshold = getValue(HALYARD.STATS_ROOT_NODE, PARTITION_THRESHOLD_PREDICATES.get(partitionType), 0);
				estimate = (sumx > 0) ? (double) (sumxx + (totalTriples - sumx) * threshold) / (double) totalTriples : 0.0;
			}

			if (estimate == 0.0) {
				// average cardinality for partitionType - assume the triples are evenly distributed over each possible value
				estimate = (double) totalTriples / (double) distinctCount;
			}

			return estimate;
		} else {
			// if there are no stats then assume the triple count is below the threshold
			return getValue(HALYARD.STATS_ROOT_NODE, PARTITION_THRESHOLD_PREDICATES.get(partitionType), defaultCardinality);
		}
	}

	/**
	 * How many triples are there with two roles known?
	 */
	private double subsetTriples(IRI graph, IRI partition1Type, Value partition1, IRI distinct1Type, IRI partition2Type, Value partition2, IRI distinct2Type, long totalTriples, long defaultCardinality) {
		if (partition1 != null) {
			return getPartitionedCardinality(graph, partition1Type, partition1, distinct1Type, totalTriples, defaultCardinality);
		} else if (partition2 != null) {
			// the number of distinct values associated to partition2 is a rough indication of the cardinality of partition1
			return getDistinctCount(graph, partition2Type, partition2, distinct2Type, defaultCardinality);
		} else {
			// geometric mean of 1 and best upper bound
			double bound1 = subsetTriples(graph, partition1Type, null, totalTriples, defaultCardinality);
			double bound2 = subsetTriples(graph, partition2Type, null, totalTriples, defaultCardinality);
			return Math.sqrt(Math.min(bound1, bound2));
		}
	}

	/**
	 * Estimates object cardinality (triples/distinct objects) for a property partition, subject cardinality (triples/distinct subjects) for an object partition, property
	 * cardinality (triples/distinct properties) for a subject partition.
	 */
	private double getPartitionedCardinality(IRI graph, IRI partitionType, @Nonnull Value partition, IRI distinctType, long totalTriples, long defaultCardinality) {
		IRI partitionIri = statsSource.getValueFactory().createIRI(partitionIriTransformer.apply(graph, partitionType, partition));
		long distinctCount = getValue(partitionIri, distinctType, -1L);
		long partitionCount = getTriplesCount(partitionIri, -1L);
		if (distinctCount != -1L && partitionCount != -1L) {
			return (double) partitionCount / (double) distinctCount;
		} else {
			return (double) totalTriples / (double) getTriplesCount(partitionIri, defaultCardinality);
		}
	}

	private double getDistinctCount(IRI graph, IRI partitionType, @Nonnull Value partition, IRI distinctType, long defaultCardinality) {
		IRI partitionIri = statsSource.getValueFactory().createIRI(partitionIriTransformer.apply(graph, partitionType, partition));
		long distinctCount = getValue(partitionIri, distinctType, -1L);
		if (distinctCount != -1L) {
			return distinctCount;
		} else {
			// geometric mean of 1 (best case estimate) and worst case estimate
			return Math.sqrt(getTriplesCount(partitionIri, defaultCardinality));
		}
	}

	private double classPartitionEntities(IRI graph, Value type, long defaultCardinality) {
		IRI partitionType = VOID.CLASS;
		IRI classPartitionIri = statsSource.getValueFactory().createIRI(partitionIriTransformer.apply(graph, partitionType, type));
		double card = getValue(classPartitionIri, VOID.ENTITIES, -1L);
		if (card == -1L) {
			// if there are no stats then assume the triple count is below the threshold
			card = getValue(HALYARD.STATS_ROOT_NODE, PARTITION_THRESHOLD_PREDICATES.get(partitionType), defaultCardinality);
		}
		return card;
	}

	@Override
	public void close() throws IOException {
		statsSource.close();
	}
}
