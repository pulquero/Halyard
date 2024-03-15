package com.msd.gin.halyard.query.algebra.evaluation;

import java.util.Objects;

public final class PartitionedIndex {
	private final IndexOrdering index;
	private final int partitionCount;

	public PartitionedIndex(IndexOrdering index, int partitionCount) {
		this.index = Objects.requireNonNull(index);
		if (partitionCount <= 0) {
			throw new IllegalArgumentException(String.format("Partition count must be greater than zero: %d", partitionCount));
		}
		this.partitionCount = partitionCount;
	}

	public IndexOrdering getIndexOrdering() {
		return index;
	}

	public int getPartitionCount() {
		return partitionCount;
	}
}
