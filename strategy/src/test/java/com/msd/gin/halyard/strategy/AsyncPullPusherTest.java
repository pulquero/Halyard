package com.msd.gin.halyard.strategy;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

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
		int taskCount = pullPusher.getThreadPoolExecutor().getActiveCount();
		assertEquals(0, taskCount);
		assertEquals(taskCount, pullPusher.getThreadPoolExecutor().getThreadDump().length);
		assertEquals(0, pullPusher.getThreadPoolExecutor().getQueueDump().length);
	}

	@Test
	public void testMXBean() throws JMException {
		MBeanServer mbs = MBeanServerFactory.newMBeanServer();
		mbs.registerMBean(pullPusher.getThreadPoolExecutor(), ObjectName.getInstance("foo:type=test"));
	}
}
