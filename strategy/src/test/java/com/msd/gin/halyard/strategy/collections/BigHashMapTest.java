package com.msd.gin.halyard.strategy.collections;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BigHashMapTest {

    @Test
    public void testBigHashMap() throws Exception {
        BigHashMap<String,String> bhm = BigHashMap.create(10);
        bhm.put("hi", "there");
        Map.Entry<String, String> entry = bhm.entrySet().iterator().next();
        assertEquals("hi", entry.getKey());
        assertEquals("there", entry.getValue());
        assertEquals(1, bhm.size());
        bhm.put("hi", "hello");
        assertEquals(1, bhm.size());
        bhm.close();
        bhm.close();
    }

    @Test(expected = IOException.class)
    public void testFailAdd() throws Exception {
        BigHashMap<String,String> bhm = BigHashMap.create(10);
        bhm.close();
        bhm.put("hi", "there");
    }

    @Test
    public void testDiskUnderLoad() throws Exception {
    	int n = 100;
        BigHashMap<Integer,String> bhm = BigHashMap.create(3);
        CountDownLatch startLock = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        for (int i=0; i<n; i++) {
        	final int k = i;
        	executor.execute(() -> {
        		try {
            		startLock.await();
					bhm.put(k, "foo");
				} catch (Exception e) {
					throw new AssertionError(e);
				}
        	});
        }
        startLock.countDown();
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        	throw new AssertionError();
        }
        assertEquals(n, bhm.size());
        int actual = 0;
        for (Object o : bhm.entrySet()) {
        	actual++;
        }
        assertEquals(n, actual);
    }
}
