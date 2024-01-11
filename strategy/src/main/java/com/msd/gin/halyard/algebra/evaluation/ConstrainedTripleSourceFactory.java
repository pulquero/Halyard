package com.msd.gin.halyard.algebra.evaluation;

import com.msd.gin.halyard.common.RDFRole;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.ValueConstraint;

import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public interface ConstrainedTripleSourceFactory {
	TripleSource getTripleSource(StatementIndex.Name indexToUse, RDFRole.Name role, int partition, int partitionBits, ValueConstraint constraint);
}
