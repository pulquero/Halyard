package com.msd.gin.halyard.util;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.*;

public class LFUCacheTest {
	@Test
	public void testEviction() {
		Map<Integer,String> cache = new LFUCache<>(4, 0.1f);
		cache.put(1, "1");
		cache.put(2, "2");
		cache.put(3, "3");
		cache.put(4, "4");
		assertEquals(4, cache.size());
		cache.get(1);
		cache.get(1);
		cache.put(5, "5");
		assertEquals(4, cache.size());
		assertEquals(Sets.newHashSet(1, 3, 4, 5), cache.keySet());
		assertTrue(cache.containsKey(1));
		cache.remove(1);
		assertFalse(cache.containsKey(1));
		cache.clear();
		assertEquals(0, cache.size());
		assertEquals(Collections.emptySet(), cache.keySet());
	}
}
