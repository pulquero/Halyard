package com.msd.gin.halyard.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.lang.ref.WeakReference;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class BlockingMultiQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
	public interface Identifiable {}
    private final Cache<Object, BlockingQueue<E>> queues = CacheBuilder.newBuilder().weakKeys().build();
    private final LinkedBlockingQueue<WeakReference<BlockingQueue<E>>> mainQueue = new LinkedBlockingQueue<>();

    private final Function<E, Object> ownerFunc;
    private final Function<Object, BlockingQueue<E>> subQueueFactory;

    public BlockingMultiQueue(Function<E, Object> ownerFunc) {
    	this(ownerFunc, o -> new LinkedBlockingQueue<>());
    }

    public BlockingMultiQueue(Function<E, Object> ownerFunc, Function<Object, BlockingQueue<E>> subQueueFactory) {
    	this.ownerFunc = ownerFunc;
    	this.subQueueFactory = subQueueFactory;
    }

    private BlockingQueue<E> getSubQueue(E e) {
    	Object owner = ownerFunc.apply(e);
    	if (owner == null) {
    		return null;
    	}

    	try {
			return queues.get(owner, () -> subQueueFactory.apply(owner));
		} catch (ExecutionException ex) {
			throw new RuntimeException(ex.getCause());
		}
    }

    private void enqueueSubQueue(BlockingQueue<E> subQueue) {
		mainQueue.add(new WeakReference<>(subQueue));
    }

    private E removeFromSubQueue(WeakReference<BlockingQueue<E>> subQueueRef) {
		BlockingQueue<E> subQueue = subQueueRef.get();
		E e;
		if (subQueue != null) {
	    	e = subQueue.remove();
		} else {
			e = null;
		}
    	return e;
    }

	@Override
	public boolean offer(E e) {
		BlockingQueue<E> subQueue = getSubQueue(e);
		if (subQueue == null) {
			return true;
		} else if (subQueue.offer(e)) {
			enqueueSubQueue(subQueue);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void put(E e) throws InterruptedException {
		BlockingQueue<E> subQueue = getSubQueue(e);
		if (subQueue != null) {
			subQueue.put(e);
			enqueueSubQueue(subQueue);
		}
	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		BlockingQueue<E> subQueue = getSubQueue(e);
		if (subQueue == null) {
			return true;
		} else if (subQueue.offer(e, timeout, unit)) {
			enqueueSubQueue(subQueue);
			return true;
		} else {
			return false;
		}
	}


	@Override
	public E take() throws InterruptedException {
		E e;
		do {
			WeakReference<BlockingQueue<E>> subQueueRef = mainQueue.take();
			e = removeFromSubQueue(subQueueRef);
		} while(e == null);
		return e;
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		E e;
		do {
			WeakReference<BlockingQueue<E>> subQueueRef = mainQueue.poll(timeout, unit);
			if (subQueueRef == null) {
				return null;
			}
			e = removeFromSubQueue(subQueueRef);
		} while(e == null);
		return e;
	}

	@Override
	public E poll() {
		E e;
		do {
			WeakReference<BlockingQueue<E>> subQueueRef = mainQueue.poll();
			if (subQueueRef == null) {
				return null;
			}
			e = removeFromSubQueue(subQueueRef);
		} while(e == null);
		return e;
	}

	@Override
	public int drainTo(Collection<? super E> c) {
        Objects.requireNonNull(c);
        if (c == this) {
            throw new IllegalArgumentException();
        }
		int count = 0;
		E e;
		while ((e = poll()) != null) {
			c.add(e);
			count++;;
		}
		return count;
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
        Objects.requireNonNull(c);
        if (c == this) {
            throw new IllegalArgumentException();
        }
		int count = 0;
		E e;
		while (count < maxElements && (e = poll()) != null) {
			c.add(e);
			count++;;
		}
		return count;
	}

	@Override
	public E peek() {
		E e;
		WeakReference<BlockingQueue<E>> subQueueRef;
		do {
			subQueueRef = mainQueue.peek();
			if (subQueueRef == null) {
				return null;
			}
			BlockingQueue<E> subQueue = subQueueRef.get();
			e = (subQueue != null) ? subQueue.peek() : null;
		} while(e == null);
		return e;
	}

	@Override
	public int size() {
		return mainQueue.size();
	}

	@Override
	public boolean isEmpty() {
		return mainQueue.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		return new Iterator<>() {
			private final Iterator<WeakReference<BlockingQueue<E>>> mainIter = mainQueue.iterator();
			private final IdentityHashMap<BlockingQueue<E>, Iterator<E>> subIters = new IdentityHashMap<>();
			private Iterator<E> current;
			private Iterator<E> removal;

			private Iterator<E> getCurrent() {
				while (current == null) {
					if (!mainIter.hasNext()) {
						return null;
					}
					WeakReference<BlockingQueue<E>> ref = mainIter.next();
					BlockingQueue<E> q = ref.get();
					if (q != null) {
						Iterator<E> iter = subIters.computeIfAbsent(q, Iterable::iterator);
						if (iter.hasNext()) {
							current = iter;
						}
					}
				}
				return current;
			}
			@Override
			public boolean hasNext() {
				Iterator<E> curr = getCurrent();
				return curr != null && curr.hasNext();
			}

			@Override
			public E next() {
				Iterator<E> curr = getCurrent();
				if (curr == null) {
					throw new NoSuchElementException();
				}
				E e = curr.next();
				removal = current;
				current = null;
				return e;
			}

			@Override
			public void remove() {
				if (removal == null) {
					throw new IllegalStateException();
				}
				mainIter.remove();
				removal.remove();
				removal = null;
			}
		};
	}

	@Override
	public int remainingCapacity() {
		return mainQueue.remainingCapacity();
	}
}
