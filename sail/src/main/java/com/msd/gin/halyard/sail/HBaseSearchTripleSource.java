/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.query.algebra.evaluation.QueryPreparer;
import com.msd.gin.halyard.sail.geosparql.WithinDistanceInterpreter;
import com.msd.gin.halyard.sail.search.KNNInterpreter;
import com.msd.gin.halyard.sail.search.SearchClient;
import com.msd.gin.halyard.sail.search.SearchDocument;
import com.msd.gin.halyard.sail.search.SearchInterpreter;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Result;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.SailException;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

public class HBaseSearchTripleSource extends HBaseTripleSource {
	private final SearchClient searchClient;

	public HBaseSearchTripleSource(KeyspaceConnection table, ValueFactory vf, StatementIndices stmtIndices, long timeoutSecs, QueryPreparer.Factory qpFactory, Map<Class<?>, ?> qhs,
			HBaseSail.ScanSettings settings, SearchClient searchClient,
			HBaseSail.Ticker ticker, int forkIndex) {
		super(table, vf, stmtIndices, timeoutSecs, qpFactory, qhs, settings, ticker, forkIndex);
		this.searchClient = Objects.requireNonNull(searchClient);
	}

	@Override
	protected void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		new SearchInterpreter().optimize(tupleExpr, dataset, bindings);
		new WithinDistanceInterpreter().optimize(tupleExpr, dataset, bindings);
		new KNNInterpreter().optimize(tupleExpr, dataset, bindings);
	}

	@Override
	protected boolean hasStatementInternal(Resource subj, IRI pred, Value obj, QueryContexts queryContexts) throws QueryEvaluationException {
		if (HalyardEvaluationStrategy.isSearchStatement(obj)) {
			return hasStatementFallback(subj, pred, obj, queryContexts);
		} else {
			return super.hasStatementInternal(subj, pred, obj, queryContexts);
		}
	}

	@Override
	protected CloseableIteration<? extends Statement> createStatementScanner(Resource subj, IRI pred, Value obj, List<Resource> contexts) throws QueryEvaluationException {
		if (HalyardEvaluationStrategy.isSearchStatement(obj)) {
			return new LiteralSearchStatementScanner(subj, pred, obj.stringValue(), contexts);
		} else {
			return super.createStatementScanner(subj, pred, obj, contexts);
		}
	}

	private static final Cache<String, List<RDFObject>> SEARCH_CACHE = Caffeine.newBuilder().maximumSize(25).expireAfterAccess(1, TimeUnit.MINUTES).build();

	// Scans the Halyard table for statements that match the specified pattern
	private class LiteralSearchStatementScanner extends StatementScanner {

		Iterator<RDFObject> objects = null;
		private final String literalSearchQuery;

		public LiteralSearchStatementScanner(Resource subj, IRI pred, String literalSearchQuery, List<Resource> contexts) throws SailException {
			super(subj, pred, null, contexts);
			this.literalSearchQuery = literalSearchQuery;
		}

		@Override
		protected Result nextResult() {
			while (true) {
				if (obj == null) {
					if (objects == null) { // perform ES query and parse results
						List<RDFObject> objectList = SEARCH_CACHE.get(literalSearchQuery, query -> {
							ArrayList<RDFObject> objList = new ArrayList<>();
							SearchResponse<? extends SearchDocument> response;
							try {
								response = searchClient.search(query, HalyardEvaluationStrategy.SEARCH_RESULT_SIZE, SearchClient.DEFAULT_MIN_SCORE, SearchClient.DEFAULT_FUZZINESS, SearchClient.DEFAULT_PHRASE_SLOP, false);
							} catch (IOException ioe) {
								throw new QueryEvaluationException(ioe);
							}
							for (Hit<? extends SearchDocument> hit : response.hits().hits()) {
								SearchDocument source = hit.source();
								Value obj = source.createValue(vf, rdfFactory);
								objList.add(rdfFactory.createObject(obj));
							}
							objList.trimToSize();
							return objList;
						});
						objects = objectList.iterator();
					}
					if (objects.hasNext()) {
						obj = objects.next();
					} else {
						return null;
					}
					contexts = contextsList.iterator(); // reset iterator over contexts
				}
				Result res = super.nextResult();
				if (res == null) {
					obj = null;
				} else {
					return res;
				}
			}
		}
	}
}
