package com.msd.gin.halyard.util;

import java.util.concurrent.BlockingQueue;

public class BlockingMultiQueueTest extends BlockingQueueTest {

	@Override
	protected BlockingQueue emptyCollection() {
		return new BlockingMultiQueue(o -> new Object());
	}
}
