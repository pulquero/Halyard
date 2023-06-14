package com.msd.gin.halyard.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryInterruptedException;

/**
 * Pushes data to a consumer.
 */
public final class QueueingBindingSetPipe extends BindingSetPipe {
    private static final Object END_OF_QUEUE = new Object();
	private final BlockingQueue<Object> queue;
	private final long offerTimeout;
	private final TimeUnit unit;
	private volatile boolean sendMore = true;

	public QueueingBindingSetPipe(int maxQueueSize, long offerTimeout, TimeUnit unit) {
		super(null);
		this.queue = new LinkedBlockingQueue<>(maxQueueSize);
		this.offerTimeout = offerTimeout;
		this.unit = unit;
	}

	public void collect(Consumer<BindingSet> consumer, long pollTimeout, TimeUnit unit) {
		boolean hasMore = true;
		try {
			while (hasMore) {
				hasMore = pollThenElse(consumer, () -> {
					throw new QueryInterruptedException(String.format("Exceeded time-out of %d%s waiting for producer", pollTimeout, toString(unit)));
				}, pollTimeout, unit);
			}
		} finally {
			stoppedPolling();
		}
	}

	public boolean pollThenElse(Consumer<BindingSet> consumer, Supplier<Boolean> timeoutAction, long pollTimeout, TimeUnit unit) {
		List<Object> nexts = poll(pollTimeout, unit);
		if (nexts != null) {
			for (Object next : nexts) {
				if (next == END_OF_QUEUE) {
					return false;
				}
				consumer.accept((BindingSet) next);
			}
			return true;
		} else {
			return timeoutAction.get();
		}
	}

    private @Nullable List<Object> poll(long pollTimeout, TimeUnit unit) {
    	List<Object> recvds = new ArrayList<Object>();
		queue.drainTo(recvds);
		if (recvds.isEmpty()) {
	    	try {
				Object o = queue.poll(pollTimeout, unit);
				if (o == null) {
					return null;
				}
				recvds = Collections.singletonList(o);
			} catch (InterruptedException ie) {
				throw new QueryInterruptedException(ie);
			}
		}
		for (Object o : recvds) {
	    	if (o instanceof Throwable) {
	    		Throwable ex = (Throwable) o;
				if (ex instanceof QueryEvaluationException) {
					throw (QueryEvaluationException) ex;
				} else {
	            	throw new QueryEvaluationException(ex);
	            }
	    	}
		}
		return recvds;
    }

	public void stoppedPolling() {
		sendMore = false;
	}

	private boolean addToQueue(Object bs) {
		if (!sendMore) {
			return false;
		}

		boolean added;
		try {
			added = queue.offer(bs, offerTimeout, unit);
			if (!added) {
				// timed-out
				try {
					// throw to generate a stack trace
					throw new QueryInterruptedException(String.format("Exceeded time-out of %d%s waiting for consumer", offerTimeout, toString(unit)));
				} catch (QueryInterruptedException e) {
					added = handleException(e);
				}
			}
		} catch (InterruptedException ie) {
			added = false;
			Thread.currentThread().interrupt();
		}
		return added;
	}

	@Override
	protected boolean next(BindingSet bs) {
		return addToQueue(bs);
	}

	@Override
	protected void doClose() {
		addToQueue(END_OF_QUEUE);
	}

    @Override
    public boolean handleException(Throwable e) {
        queue.clear();
        if (!addToQueue(e)) {
        	// report problem
        	throw new RuntimeException(e);
        }
        return false;
    }

	@Override
	public String toString() {
		return "Pipe "+Integer.toHexString(this.hashCode())+" for queue "+Integer.toHexString(queue.hashCode());
	}

	private static String toString(TimeUnit unit) {
		switch (unit) {
			case NANOSECONDS:
				return "ns";
			case MICROSECONDS:
				return "μs";
			case MILLISECONDS:
				return "ms";
			case SECONDS:
				return "s";
			case MINUTES:
				return "min";
			case HOURS:
				return "hr";
			case DAYS:
				return "d";
			default:
				throw new IllegalArgumentException(String.format("%s not yet supported", unit));
		}
	}
}
