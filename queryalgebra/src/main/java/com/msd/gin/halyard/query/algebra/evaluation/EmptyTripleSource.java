package com.msd.gin.halyard.query.algebra.evaluation;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public class EmptyTripleSource implements TripleSource {
	private final ValueFactory vf;

	public EmptyTripleSource() {
		this(SimpleValueFactory.getInstance());
	}

	public EmptyTripleSource(ValueFactory vf) {
		this.vf = vf;
	}

	@Override
	public CloseableIteration<? extends Statement> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
		return EMPTY_ITERATION;
	}

	@Override
	public ValueFactory getValueFactory() {
		return vf;
	}

}
