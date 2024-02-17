/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.strategy.collections;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class BigHashSetTest {

    @Test
    public void testBigHashSet() throws Exception {
        BigHashSet<String> bhs = BigHashSet.create(10);
        bhs.add("hi");
        assertEquals("hi", bhs.iterator().next());
        assertTrue(bhs.contains("hi"));
        assertEquals(1, bhs.size());
        bhs.add("hi");
        assertEquals(1, bhs.size());
        bhs.close();
        bhs.close();
    }

    @Test(expected = IOException.class)
    public void testFailAdd() throws Exception {
        BigHashSet<String> bhs = BigHashSet.create(10);
        bhs.close();
        bhs.add("hi");
    }

    @Test(expected = IOException.class)
    public void testFailContains() throws Exception {
        BigHashSet<String> bhs = BigHashSet.create(10);
        bhs.close();
        bhs.contains("hi");
    }

    @Test
    public void testDiskUnderLoad() throws Exception {
    	int n = 100;
        BigHashSet<Integer> bhs = BigHashSet.create(3);
        CountDownLatch startLock = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        for (int i=0; i<n; i++) {
        	final int k = i;
        	executor.execute(() -> {
        		try {
            		startLock.await();
					bhs.add(k);
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
        assertEquals(n, bhs.size());
        int actual = 0;
        for (Integer i : bhs) {
        	actual++;
        }
        assertEquals(n, actual);
    }
}
