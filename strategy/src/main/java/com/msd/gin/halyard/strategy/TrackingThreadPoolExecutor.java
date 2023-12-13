package com.msd.gin.halyard.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class TrackingThreadPoolExecutor extends ThreadPoolExecutor implements TrackingThreadPoolExecutorMXBean {
	private final ConcurrentHashMap<Thread, Runnable> runningTasks;

	public TrackingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		runningTasks = new ConcurrentHashMap<>(maximumPoolSize);
	}

	public Map<Thread, Runnable> getActiveTasks() {
		return Collections.unmodifiableMap(runningTasks);
	}

	@Override
	protected void beforeExecute(Thread t, Runnable r) {
		runningTasks.put(t, r);
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		runningTasks.remove(Thread.currentThread());
	}

	@Override
	public int getQueueSize() {
		return getQueue().size();
	}

	@Override
	public ThreadInfo[] getThreadDump() {
		// NB: the size is only approximate as the contents of the map is under constant change!!!
		List<ThreadInfo> dump = new ArrayList<>(runningTasks.size());
		for (Map.Entry<Thread, Runnable> entry : getActiveTasks().entrySet()) {
			Thread t = entry.getKey();
			Runnable r = entry.getValue();
			dump.add(new ThreadInfo(t.getName(), t.getState(), r.toString()));
		}
		ThreadInfo[] thrInfos = dump.toArray(new ThreadInfo[dump.size()]);
		Arrays.sort(thrInfos, new Comparator<ThreadInfo>() {
			@Override
			public int compare(ThreadInfo o1, ThreadInfo o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return thrInfos;
	}

	@Override
	public QueueInfo[] getQueueDump() {
		return getQueueDump(10);
	}

	private QueueInfo[] getQueueDump(int n) {
		BlockingQueue<Runnable> taskQueue = getQueue();
		Runnable[] tasks = taskQueue.toArray(new Runnable[0]);
		if (taskQueue instanceof PriorityBlockingQueue<?>) {
			Comparator<?> comparator = ((PriorityBlockingQueue<?>) taskQueue).comparator();
			// need to sort as contents are in no particular order
			Arrays.sort(tasks, (Comparator<Runnable>) comparator);
		}
		n = Math.min(n, tasks.length);
		QueueInfo[] infos = new QueueInfo[n];
		for (int i=0; i<n; i++) {
			infos[i] = new QueueInfo(tasks[i].toString());
		}
		return infos;
	}

	@Override
	public String toString() {
		int n = 10;
		StringBuilder buf = new StringBuilder(super.toString());
		buf.append("\nThreads:\n");
		for (ThreadInfo ti : getThreadDump()) {
			buf.append("  ").append(ti).append("\n");
		}
		buf.append("\nQueue (first " + n + " of ~" + getQueue().size() + "):\n");
		int i = 0;
		for (QueueInfo qi : getQueueDump(n)) {
			buf.append("  ").append(++i).append(": ").append(qi).append("\n");
		}
		return buf.toString();
	}
}