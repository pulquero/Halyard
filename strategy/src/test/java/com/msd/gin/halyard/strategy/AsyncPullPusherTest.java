package com.msd.gin.halyard.strategy;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AsyncPullPusherTest {
	private AsyncPullPusher pullPusher;

	@BeforeEach
	public void setUp() {
		pullPusher = new AsyncPullPusher("test", new Configuration());
	}

	@AfterEach
	public void tearDown() {
		pullPusher.close();
	}

	@Test
	public void testThreadPool() {
		int tasks = pullPusher.getThreadPoolExecutor().getActiveCount();
		assertEquals(0, tasks);
		assertEquals(tasks, pullPusher.getThreadPoolExecutor().getThreadDump().length);
		assertEquals(0, pullPusher.getThreadPoolExecutor().getQueueDump().length);
	}
}
