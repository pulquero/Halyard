package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.ValueIdentifier;

import java.io.IOException;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;

@MetaInfServices(TupleFunction.class)
public final class PredicateTupleFunction extends AbstractReificationTupleFunction {

	@Override
	public String getURI() {
		return RDF.PREDICATE.stringValue();
	}

	@Override
	protected int statementPosition() {
		return 1;
	}

	@Override
	protected Value getValue(KeyspaceConnection ks, ValueIdentifier id, ValueFactory vf, StatementIndices stmtIndices) throws IOException {
		return stmtIndices.getPredicate(ks, id, vf);
	}
}
