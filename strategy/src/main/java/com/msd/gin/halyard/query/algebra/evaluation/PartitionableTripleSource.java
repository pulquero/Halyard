package com.msd.gin.halyard.query.algebra.evaluation;

import com.msd.gin.halyard.common.RDFRole;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.ValueConstraint;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public interface PartitionableTripleSource {
	int getPartitionIndex();
	TripleSource partition(RDFRole.Name role, @Nullable StatementIndex.Name indexToPartition, int partitionCount, ValueConstraint constraint);
}
