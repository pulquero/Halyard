package com.msd.gin.halyard.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BlockingMultiQueueExtendedTest {

	protected BlockingMultiQueue<MockElement> emptyQueue() {
		return new BlockingMultiQueue<>(m -> m.getQueueOwner());
	}

	@Test
	public void testIterator() {
		Object o1 = new Object();
		Object o2 = new Object();
		List<MockElement> expectedValues = new ArrayList<>();
		expectedValues.add(new MockElement(o1));
		expectedValues.add(new MockElement(o1));
		expectedValues.add(new MockElement(o2));
		expectedValues.add(new MockElement(o1));

		BlockingMultiQueue<MockElement> q = emptyQueue();
		for (MockElement m : expectedValues) {
			q.add(m);
		}
		assertIterableEquals(expectedValues, q);
	}

	@Test
	public void testPeekRemove() {
		Object o1 = new Object();
		Object o2 = new Object();
		List<MockElement> expectedValues = new ArrayList<>();
		expectedValues.add(new MockElement(o1));
		expectedValues.add(new MockElement(o1));
		expectedValues.add(new MockElement(o2));
		expectedValues.add(new MockElement(o1));

		BlockingMultiQueue<MockElement> q = emptyQueue();
		for (MockElement m : expectedValues) {
			q.add(m);
		}

		Iterator<MockElement> expectedIter = expectedValues.iterator();
		while (!q.isEmpty()) {
			assertTrue(expectedIter.hasNext());
			Object actual = q.peek();
			Object expected = expectedIter.next();
			assertEquals(expected, actual);
			assertEquals(expected, q.remove());
		}
		assertFalse(expectedIter.hasNext());
	}


	static class MockElement {
		final Object owner;

		MockElement(Object owner) {
			this.owner = owner;
		}

		public Object getQueueOwner() {
			return owner;
		}
	}
}
