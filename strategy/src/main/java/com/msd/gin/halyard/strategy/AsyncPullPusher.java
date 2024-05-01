package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.query.BindingSetPipe;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AsyncPullPusher implements PullPusher {
	private static final Logger LOGGER = LoggerFactory.getLogger(AsyncPullPusher.class);

	private static TrackingThreadPoolExecutor createExecutor(String namePrefix, int threads, int queueSize) {
		AtomicInteger threadSeq = new AtomicInteger();
		ThreadFactory tf = (r) -> {
			Thread thr = new Thread(r, namePrefix+threadSeq.incrementAndGet());
			thr.setDaemon(true);
			thr.setUncaughtExceptionHandler((t,e) -> LOGGER.warn("Thread {} exited due to an uncaught exception", t.getName(), e));
			return thr;
		};
		// fixed-size thread pool that can wind down when idle
		TrackingThreadPoolExecutor executor = new TrackingThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS, new PriorityBlockingQueue<>(queueSize), tf);
		executor.allowCoreThreadTimeOut(true);
		return executor;
	}

	private final TupleExprPriorityAssigner priorityAssigner = new TupleExprPriorityAssigner();
	private final TrackingThreadPoolExecutor executor;
	private int taskQueueMaxSize;
	private double pullPushAllLimit;

	AsyncPullPusher(String name, Configuration conf) {
	    int threads = conf.getInt(StrategyConfig.HALYARD_EVALUATION_THREADS, StrategyConfig.DEFAULT_THREADS);
	    taskQueueMaxSize = conf.getInt(StrategyConfig.HALYARD_EVALUATION_TASK_QUEUE_MAX_SIZE, StrategyConfig.DEFAULT_TASK_QUEUE_MAX_SIZE);
		executor = createExecutor(name + " ", threads, taskQueueMaxSize);
		int limit = conf.getInt(StrategyConfig.HALYARD_EVALUATION_PULL_PUSH_ASYNC_ALL_LIMIT, StrategyConfig.DEFAULT_PULL_PUSH_ASYNC_ALL_LIMIT);
		setPullPushAllLimit(limit);
	}

	TrackingThreadPoolExecutorMXBean getThreadPoolExecutorMXBean() {
		return executor;
	}

	void setPullPushAllLimit(int limit) {
		pullPushAllLimit = (limit != -1) ? limit : Double.POSITIVE_INFINITY;
	}

	int getPullPushAllLimit() {
		return (pullPushAllLimit != Double.POSITIVE_INFINITY) ? (int) pullPushAllLimit : -1;
	}

	void setTaskQueueMaxSize(int size) {
		taskQueueMaxSize = size;
	}

	int getTaskQueueMaxSize() {
		return taskQueueMaxSize;
	}

	/**
     * Asynchronously pulls from an iteration of binding sets and pushes to a {@link BindingSetPipe}.
     * @param pipe the pipe that evaluation results are returned on
     * @param evalStep query step to evaluate
     * @param node an implementation of any {@TupleExpr} sub-type
     * @param bs binding set
     * @param trackerFactory
     */
	@Override
	public void pullPush(BindingSetPipe pipe,
			QueryEvaluationStep evalStep,
			TupleExpr node, BindingSet bs,
			Function<CloseableIteration<BindingSet, QueryEvaluationException>,CloseableIteration<BindingSet, QueryEvaluationException>> trackerFactory) {
		double sizeEstimate = node.getResultSizeEstimate();
		if (sizeEstimate == -1) {
			// if unknown assume it is very large
			sizeEstimate = Double.MAX_VALUE;
		} else if (sizeEstimate == 0) {
			// it's an estimate so even zero may not be zero
			sizeEstimate = 1;
		}
		PrioritizedTask task;
		if (sizeEstimate <= pullPushAllLimit) {
			task = new IterateAllAndPipeTask(pipe, evalStep, node, bs, trackerFactory);
		} else {
			task = new IterateSingleAndPipeTask(pipe, evalStep, node, bs, trackerFactory);
		}
		if (!submit(task)) {
			task.run();
		}
    }

	@Override
	public int getActiveCount() {
		return executor.getActiveCount();
	}

	@Override
	public int getQueueSize() {
		return executor.getQueueSize();
	}

	@Override
	public void close() {
		executor.shutdownNow();
	}

	private boolean submit(PrioritizedTask task) {
		if (executor.getQueueSize() <= taskQueueMaxSize) {
			executor.execute(task);
			return true;
		} else {
			return false;
		}
	}


	abstract class PrioritizedTask implements Comparable<PrioritizedTask>, Runnable {
    	static final int MIN_SUB_PRIORITY = 0;
    	static final int MAX_SUB_PRIORITY = 99999;
    	final TupleExpr queryNode;
    	final BindingSet bindingSet;
    	final int queryPriority;
    	int taskPriority;

    	PrioritizedTask(TupleExpr queryNode, BindingSet bs) {
    		this.queryNode = queryNode;
    		this.bindingSet = bs;
    		this.queryPriority = priorityAssigner.getPriority(queryNode);
    		setSubPriority(MIN_SUB_PRIORITY);
    	}

    	/**
    	 * Sets this task's sub-priority.
    	 * @param subPriority MIN_SUB_PRIORITY to MAX_SUB_PRIORITY inclusive
    	 */
    	protected final void setSubPriority(int subPriority) {
    		taskPriority = (MAX_SUB_PRIORITY+1)*queryPriority + subPriority;
    	}

    	@Override
		public final int compareTo(PrioritizedTask o) {
    		// descending order
			return o.taskPriority - this.taskPriority;
		}

    	@Override
    	public String toString() {
    		return super.toString() + "[queryNode = " + HalyardEvaluationExecutor.printQueryNode(queryNode, bindingSet) + ", priority = " + taskPriority + "]";
    	}
	}

	final class IterateSingleAndPipeTask extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final QueryEvaluationStep evalStep;
        private final Function<CloseableIteration<BindingSet, QueryEvaluationException>,CloseableIteration<BindingSet, QueryEvaluationException>> trackerFactory;
        private int pushPriority = MIN_SUB_PRIORITY;
        private CloseableIteration<BindingSet, QueryEvaluationException> iter;

        /**
         * Constructor for the class with the supplied variables
         * @param pipe The pipe to return evaluations to
         * @param evalStep The query step to evaluation
         * @param expr
         * @param bs
         * @param trackerFactory
         */
		IterateSingleAndPipeTask(BindingSetPipe pipe,
				QueryEvaluationStep evalStep,
				TupleExpr expr, BindingSet bs,
				Function<CloseableIteration<BindingSet, QueryEvaluationException>,CloseableIteration<BindingSet, QueryEvaluationException>> trackerFactory) {
			super(expr, bs);
            this.pipe = pipe;
            this.evalStep = evalStep;
            this.trackerFactory = trackerFactory;
        }

		boolean pushNext() {
        	try {
            	if (!pipe.isClosed()) {
            		if (iter == null) {
                        iter = trackerFactory.apply(evalStep.evaluate(bindingSet));
            		}
                	if(iter.hasNext()) {
                        BindingSet bs = iter.next();
                        if (pipe.push(bs)) { //true indicates more data is expected from this iterator
                            return true;
                        }
            		}
            	}
            } catch (Throwable e) {
            	// propagate exception
            	boolean doNext = pipe.handleException(e);
            	// if we have an iterator then keep going and pull next
                if (iter != null && doNext) {
                	return true;
                }
            }
        	// close iter first to immediately release resources as pipe.close() maybe non-trivial (e.g. DISTINCT)
        	if (iter != null) {
        		try {
        			iter.close();
        		} catch (QueryEvaluationException ignore) {
        		}
        	}
        	pipe.close();
        	return false;
		}

		@Override
    	public void run() {
        	while (pushNext()) {
        		if (pushPriority < MAX_SUB_PRIORITY) {
        			pushPriority++;
        		}
        		setSubPriority(pushPriority);
        		// if successfully submitted for async execution
                if (submit(this)) {
                	return;
                }
        	}
    	}
    }

    final class IterateAllAndPipeTask extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final QueryEvaluationStep evalStep;
        private final Function<CloseableIteration<BindingSet, QueryEvaluationException>,CloseableIteration<BindingSet, QueryEvaluationException>> trackerFactory;

        /**
         * Constructor for the class with the supplied variables
         * @param pipe The pipe to return evaluations to
         * @param evalStep The query step to evaluation
         * @param expr
         * @param bs
         * @param trackerFactory
         */
		IterateAllAndPipeTask(BindingSetPipe pipe,
				QueryEvaluationStep evalStep,
				TupleExpr expr, BindingSet bs,
				Function<CloseableIteration<BindingSet, QueryEvaluationException>,CloseableIteration<BindingSet, QueryEvaluationException>> trackerFactory) {
			super(expr, bs);
            this.pipe = pipe;
            this.evalStep = evalStep;
            this.trackerFactory = trackerFactory;
        }

		@Override
    	public void run() {
			SyncPullPusher.pullPushAll(pipe, evalStep, queryNode, bindingSet, trackerFactory);
    	}
    }
}