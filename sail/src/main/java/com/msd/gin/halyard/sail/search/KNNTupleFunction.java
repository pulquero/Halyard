package com.msd.gin.halyard.sail.search;

import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.model.ObjectArrayLiteral;
import com.msd.gin.halyard.model.ObjectLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.function.ExtendedTupleFunction;

import java.io.IOException;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.SearchResponse;

@MetaInfServices(TupleFunction.class)
public class KNNTupleFunction implements ExtendedTupleFunction {
	private static final Logger LOGGER = LoggerFactory.getLogger(KNNTupleFunction.class);

	@Override
	public String getURI() {
		return HALYARD.KNN_FUNCTION.stringValue();
	}

	@Override
	public CloseableIteration<? extends List<? extends Value>> evaluate(TripleSource tripleSource, Value... args) throws QueryEvaluationException {
		ExtendedTripleSource extTripleSource = (ExtendedTripleSource) tripleSource;

		if (args.length != 5) {
			throw new QueryEvaluationException("Missing arguments");
		}

		if (!args[0].isLiteral() || !HALYARD.ARRAY_TYPE.equals(((Literal) args[0]).getDatatype())) {
			throw new QueryEvaluationException("Invalid query value");
		}
		int argPos = 0;
		Object[] query = ObjectArrayLiteral.objectArray((Literal) args[argPos++]);
		Float[] vec = new Float[query.length];
		for (int i = 0; i < query.length; i++) {
			vec[i] = ((Number) query[i]).floatValue();
		}
		int k = ((Literal) args[argPos++]).intValue();
		int numCandidates = ((Literal) args[argPos++]).intValue();
		double minScore = ((Literal) args[argPos++]).doubleValue();
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
			SearchResponse<? extends SearchDocument> searchResults = searchClient.knn(vec, k, numCandidates, minScore, hasAdditionalFields);
			return SearchTupleFunction.transformResults(searchResults, matches, valueFactory, rdfFactory);
		} catch (ElasticsearchException e) {
			LOGGER.error(String.format("Query failed: %s", (Object) query));
			throw new QueryEvaluationException(e);
		} catch (IOException e) {
			throw new QueryEvaluationException(e);
		}
	}
}
