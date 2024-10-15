package com.msd.gin.halyard.sail.search;

import com.google.common.collect.Lists;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.model.FloatArrayLiteral;
import com.msd.gin.halyard.model.ObjectArrayLiteral;
import com.msd.gin.halyard.model.ObjectLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.function.ExtendedTupleFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

@MetaInfServices(TupleFunction.class)
public class SearchTupleFunction implements ExtendedTupleFunction {
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchTupleFunction.class);

	@Override
	public String getURI() {
		return HALYARD.SEARCH.stringValue();
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>> evaluate(TripleSource tripleSource, Value... args) throws QueryEvaluationException {
		ExtendedTripleSource extTripleSource = (ExtendedTripleSource) tripleSource;

		if (args.length != 6) {
			throw new QueryEvaluationException("Missing arguments");
		}

		if (!args[0].isLiteral()) {
			throw new QueryEvaluationException("Invalid query value");
		}
		int argPos = 0;
		String query = ((Literal) args[argPos++]).getLabel();
		int limit = ((Literal) args[argPos++]).intValue();
		double minScore = ((Literal) args[argPos++]).doubleValue();
		int fuzziness = ((Literal) args[argPos++]).intValue();
		int phraseSlop = ((Literal) args[argPos++]).intValue();
		List<MatchParams> matches = ((ObjectLiteral<List<MatchParams>>) args[argPos++]).objectValue();
		ValueFactory valueFactory = extTripleSource.getValueFactory();
		StatementIndices indices = extTripleSource.getQueryHelper(StatementIndices.class);
		RDFFactory rdfFactory = indices.getRDFFactory();
		SearchClient searchClient = extTripleSource.getQueryHelper(SearchClient.class);

		boolean hasAdditionalFields = false;
		for (MatchParams matchParams : matches) {
			if (!matchParams.fields.isEmpty()) {
				hasAdditionalFields = true;
				break;
			}
		}

		try {
			SearchResponse<? extends SearchDocument> searchResults = searchClient.search(query, limit, minScore, fuzziness, phraseSlop, hasAdditionalFields);
			return transformResults(searchResults, matches, valueFactory, rdfFactory);
		} catch (ElasticsearchException e) {
			LOGGER.error(String.format("Query failed: %s", query));
			throw new QueryEvaluationException(e);
		} catch (IOException e) {
			throw new QueryEvaluationException(e);
		}
	}

	static CloseableIteration<? extends List<? extends Value>> transformResults(SearchResponse<? extends SearchDocument> searchResults, List<MatchParams> matches, ValueFactory valueFactory, RDFFactory rdfFactory) {
		List<List<Hit<? extends SearchDocument>>> results;
		final int numMatchValues = matches.size();
		if (numMatchValues == 1) {
			results = Lists.transform(searchResults.hits().hits(), doc -> Collections.singletonList(doc));
		} else {
			// in case anyone actually does this
			results = Lists.cartesianProduct(Collections.nCopies(numMatchValues, searchResults.hits().hits()));
		}
		return new ConvertingIteration<List<Hit<? extends SearchDocument>>, List<Value>>(new CloseableIteratorIteration<List<Hit<? extends SearchDocument>>>(results.iterator())) {
			int outputSize = 2;

			@Override
			protected List<Value> convert(List<Hit<? extends SearchDocument>> matchValues) throws QueryEvaluationException {
				List<Value> values = new ArrayList<>(outputSize);
				for (int i = 0; i < numMatchValues; i++) {
					Hit<? extends SearchDocument> matchValue = matchValues.get(i);
					SearchDocument doc = matchValue.source();
					MatchParams matchParams = matches.get(i);
					if (!matchParams.valueVars.isEmpty()) {
						Value value = doc.createValue(valueFactory, rdfFactory);
						for (int k = 0; k < matchParams.valueVars.size(); k++) {
							values.add(value);
						}
					}
					if (!matchParams.scoreVars.isEmpty()) {
						Literal score = valueFactory.createLiteral(matchValue.score());
						for (int k = 0; k < matchParams.scoreVars.size(); k++) {
							values.add(score);
						}
					}
					if (!matchParams.indexVars.isEmpty()) {
						Literal index = valueFactory.createLiteral(matchValue.index());
						for (int k = 0; k < matchParams.indexVars.size(); k++) {
							values.add(index);
						}
					}
					for (MatchParams.FieldParams fieldParams : matchParams.fields) {
						Literal l;
						if (SearchDocument.VECTOR_FIELD.equals(fieldParams.name)) {
							l = new FloatArrayLiteral(doc.vector);
						} else {
							Object v = doc.getAdditionalField(fieldParams.name);
							if (v instanceof List<?>) {
								l = new ObjectArrayLiteral(((List<?>) v).toArray());
							} else if (v != null) {
								l = Values.literal(valueFactory, v, false);
							} else {
								l = null;
							}
						}
						for (int k = 0; k < fieldParams.valueVars.size(); k++) {
							values.add(l);
						}
					}
				}
				outputSize = values.size();
				return values;
			}
		};
	}
}
