package com.msd.gin.halyard.algebra.evaluation;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public interface ExtendedTripleSource extends TripleSource {
	default boolean hasStatement(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		try (CloseableIteration<? extends Statement, QueryEvaluationException> iter = getStatements(subj, pred, obj, contexts)) {
			return iter.hasNext();
		}
	}

	QueryPreparer getQueryPreparer();
}
