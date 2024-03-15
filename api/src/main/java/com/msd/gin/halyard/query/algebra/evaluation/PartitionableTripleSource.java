package com.msd.gin.halyard.query.algebra.evaluation;

import com.msd.gin.halyard.model.TermRole;
import com.msd.gin.halyard.model.ValueConstraint;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

public interface PartitionableTripleSource {
	int getPartitionIndex();
	TripleSource partition(TermRole role, @Nullable PartitionedIndex indexToPartition, @Nullable ValueConstraint constraint);
}
