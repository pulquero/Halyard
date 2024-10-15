package com.msd.gin.halyard.sail.model.embedding.function;

import com.msd.gin.halyard.model.FloatArrayLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.evaluation.ExtendedTripleSource;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

@MetaInfServices(Function.class)
public class VectorEmbedding implements Function {

	@Override
	public String getURI() {
		return HALYARD.VECTOR_EMBEDDING_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Value evaluate(TripleSource ts, Value... args) throws ValueExprEvaluationException {
		if (args.length != 1) {
			throw new QueryEvaluationException("Missing arguments");
		}

		if (!args[0].isLiteral()) {
			throw new QueryEvaluationException(String.format("Non-literal value: %s", args[0]));
		}
		Literal l = (Literal) args[0];

		ExtendedTripleSource extTs = (ExtendedTripleSource) ts;
		Response<Embedding> resp = extTs.getQueryHelper(EmbeddingModel.class).embed(l.getLabel());
		float[] vec = resp.content().vector();
		return new FloatArrayLiteral(vec);
	}
}
