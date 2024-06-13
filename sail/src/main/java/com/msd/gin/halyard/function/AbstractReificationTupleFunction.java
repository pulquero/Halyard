package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.ByteUtils;
import com.msd.gin.halyard.common.KeyspaceConnection;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.ValueIdentifier;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.evaluation.function.ExtendedTupleFunction;
import com.msd.gin.halyard.sail.HBaseTripleSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

public abstract class AbstractReificationTupleFunction implements ExtendedTupleFunction {

	protected abstract int statementPosition();

	protected abstract Value getValue(KeyspaceConnection ks, ValueIdentifier id, ValueFactory vf, StatementIndices stmtIndices) throws IOException;

	@Override
	public final CloseableIteration<? extends List<? extends Value>> evaluate(TripleSource tripleSource,
			Value... args)
		throws ValueExprEvaluationException
	{
		HBaseTripleSource extTripleSource = (HBaseTripleSource) tripleSource;

		if (args.length != 1 || !(args[0] instanceof IRI)) {
			throw new ValueExprEvaluationException(String.format("%s requires an identifier IRI", getURI()));
		}

		StatementIndices indices = extTripleSource.getStatementIndices();
		RDFFactory rdfFactory = indices.getRDFFactory();

		IRI idIri = (IRI) args[0];
		ValueIdentifier id;
		if (HALYARD.STATEMENT_ID_NS.getName().equals(idIri.getNamespace())) {
			int idSize = rdfFactory.getIdSize();
			byte[] stmtId = ByteUtils.decode(idIri.getLocalName());
			byte[] idBytes = new byte[idSize];
			System.arraycopy(stmtId, statementPosition() * idSize, idBytes, 0, idSize);
			id = rdfFactory.id(idBytes);
		} else if (HALYARD.VALUE_ID_NS.getName().equals(idIri.getNamespace())) {
			id = rdfFactory.id(ByteUtils.decode(idIri.getLocalName()));
		} else {
			throw new ValueExprEvaluationException(String.format("%s requires an identifier IRI", getURI()));
		}

		KeyspaceConnection keyspace = extTripleSource.getKeyspaceConnection();
		Value v;
		try {
			v = getValue(keyspace, id, extTripleSource.getValueFactory(), indices);
		} catch (IOException e) {
			throw new ValueExprEvaluationException(e);
		}
		if (v != null) {
			return new SingletonIteration<>(Collections.singletonList(v));
		} else {
			return new EmptyIteration<>();
		}
	}
}
