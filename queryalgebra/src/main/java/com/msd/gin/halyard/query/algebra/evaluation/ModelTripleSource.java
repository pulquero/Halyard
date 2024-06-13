package com.msd.gin.halyard.query.algebra.evaluation;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;

public class ModelTripleSource implements CloseableTripleSource {

	private final Model model;
	private final ValueFactory vf;

	public ModelTripleSource(Model m) {
		this(m, SimpleValueFactory.getInstance());
	}

	public ModelTripleSource(Model m, ValueFactory vf) {
		this.model = m;
		this.vf = vf;
	}

	@Override
	public CloseableIteration<? extends Statement> getStatements(Resource subj, IRI pred,
			Value obj, Resource... contexts) throws QueryEvaluationException {
		return new CloseableIteratorIteration<>(model.getStatements(subj, pred, obj, contexts).iterator());
	}

	@Override
	public ValueFactory getValueFactory() {
		return vf;
	}

	@Override
	public void close() {
	}
}
