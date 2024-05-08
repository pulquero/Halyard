package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.sail.HBaseTripleSource;
import com.msd.gin.halyard.sail.HalyardStatsBasedStatementPatternCardinalityCalculator;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public class DatasetIri implements Function {

	@Override
	public String getURI() {
		return HALYARD.DATASET_IRI_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Value evaluate(TripleSource tripleSource, Value... args) throws ValueExprEvaluationException {
		if (args.length != 3 || !args[0].isIRI() || !args[1].isIRI()) {
			throw new ValueExprEvaluationException(String.format("%s requires a graph IRI, dataset type IRI and a value", getURI()));
		}

		HalyardStatsBasedStatementPatternCardinalityCalculator.PartitionIriTransformer partitionIriTransformer;
		if (tripleSource instanceof HBaseTripleSource) {
			HBaseTripleSource extTripleSource = (HBaseTripleSource) tripleSource;
			StatementIndices indices = extTripleSource.getStatementIndices();
			RDFFactory rdfFactory = indices.getRDFFactory();
			partitionIriTransformer = HalyardStatsBasedStatementPatternCardinalityCalculator.createPartitionIriTransformer(rdfFactory);
		} else {
			partitionIriTransformer = HalyardStatsBasedStatementPatternCardinalityCalculator.createSimplePartitionIriTransformer();
		}

		return tripleSource.getValueFactory().createIRI(partitionIriTransformer.apply((IRI) args[0], (IRI) args[1], args[2]));
	}
}
