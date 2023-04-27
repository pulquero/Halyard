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
package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.QueueingBindingSetPipe;
import com.msd.gin.halyard.util.MBeanDetails;
import com.msd.gin.halyard.util.MBeanManager;
import com.msd.gin.halyard.util.RateTracker;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HalyardEvaluationExecutor implements HalyardEvaluationExecutorMXBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(HalyardEvaluationExecutor.class);
    private static final Timer TIMER = new Timer("HalyardEvaluationExecutorTimer", true);

    private static TrackingThreadPoolExecutor createExecutor(String groupName, String namePrefix, int threads) {
		ThreadGroup tg = new ThreadGroup(groupName);
		AtomicInteger threadSeq = new AtomicInteger();
		ThreadFactory tf = (r) -> {
			Thread thr = new Thread(tg, r, namePrefix+threadSeq.incrementAndGet());
			thr.setDaemon(true);
			return thr;
		};
		// fixed-size thread pool that can wind down when idle
		TrackingThreadPoolExecutor executor = new TrackingThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS, new PriorityBlockingQueue<>(64), tf);
		executor.allowCoreThreadTimeOut(true);
		return executor;
	}

    private final RateTracker taskRateTracker;
	private MBeanManager<HalyardEvaluationExecutor> mbeanManager;
	private final TimerTask registerMBeanTask;

    private int threads;
    private int maxRetries;
    private int retryLimit;
    private int threadGain;
    private int maxThreads;
    private float minTaskRate;

	private final TrackingThreadPoolExecutor executor;

    private int maxQueueSize;
	private int pollTimeoutMillis;
	private int offerTimeoutMillis;

	public HalyardEvaluationExecutor(Configuration conf, Map<String,String> connAttrs) {
	    threads = conf.getInt(StrategyConfig.HALYARD_EVALUATION_THREADS, 10);
	    setMaxRetries(conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_RETRIES, 3));
	    setRetryLimit(conf.getInt(StrategyConfig.HALYARD_EVALUATION_RETRY_LIMIT, 100));
	    threadGain = conf.getInt(StrategyConfig.HALYARD_EVALUATION_THREAD_GAIN, 5);
	    maxThreads = conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_THREADS, 100);
	    minTaskRate = conf.getFloat(StrategyConfig.HALYARD_EVALUATION_MIN_TASK_RATE, 0.1f);
		executor = createExecutor("Halyard Executors", "Halyard ", threads);

	    maxQueueSize = conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_QUEUE_SIZE, 5000);
		pollTimeoutMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_POLL_TIMEOUT_MILLIS, 1000);
		offerTimeoutMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_OFFER_TIMEOUT_MILLIS, conf.getInt("hbase.client.scanner.timeout.period", 60000));

		int taskRateUpdateMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_TASK_RATE_UPDATE_MILLIS, 100);
		int taskRateWindowSize = conf.getInt(StrategyConfig.HALYARD_EVALUATION_TASK_RATE_WINDOW_SIZE, 10);
		taskRateTracker = new RateTracker(TIMER, taskRateUpdateMillis, taskRateWindowSize, () -> executor.getCompletedTaskCount());
		taskRateTracker.start();

		long threadPoolCheckPeriodSecs = conf.getInt(StrategyConfig.HALYARD_EVALUATION_THREAD_POOL_CHECK_PERIOD_SECS, 5);
		TIMER.schedule(new TimerTask() {
			@Override
			public void run() {
        		final boolean overallProgress = taskRateTracker.getRatePerSecond() > minTaskRate;
        		if (overallProgress) {
        			int active = executor.getActiveCount();
        			if (active > threads) {
        				// we are making good progress and have excess threads
        				if (active <= executor.getMaximumPoolSize()) { // no outstanding threads to be reclaimed
			    			synchronized (executor) {
			    				int corePoolSize = executor.getCorePoolSize();
			    				if (corePoolSize > threads) {
			    					corePoolSize--;
			    					executor.setCorePoolSize(corePoolSize);
			    				}
			    				int maxPoolSize = executor.getMaximumPoolSize();
			    				if (maxPoolSize > threads) {
			    					maxPoolSize--;
			    					executor.setMaximumPoolSize(Math.max(maxPoolSize, corePoolSize));
			    				}
			    			}
        				}
        			}
        		}
			}
		}, 1000L, TimeUnit.SECONDS.toMillis(threadPoolCheckPeriodSecs));

		// don't both registering MBeans for short-lived queries
		registerMBeanTask = new TimerTask() {
			@Override
			public void run() {
				mbeanManager = new MBeanManager<>() {
					@Override
					protected List<MBeanDetails> mbeans(HalyardEvaluationExecutor executor) {
						List<MBeanDetails> mbeanObjs = new ArrayList<>(2);
						{
							Hashtable<String,String> attrs = new Hashtable<>();
							attrs.putAll(connAttrs);
							mbeanObjs.add(new MBeanDetails(executor, HalyardEvaluationExecutorMXBean.class, attrs));
						}
						{
							Hashtable<String,String> attrs = new Hashtable<>();
							attrs.putAll(connAttrs);
							mbeanObjs.add(new MBeanDetails(executor.executor, TrackingThreadPoolExecutorMXBean.class, attrs));
						}
						return mbeanObjs;
					}
				};
				mbeanManager.register(HalyardEvaluationExecutor.this);
			}
		};
		TIMER.schedule(registerMBeanTask, TimeUnit.MINUTES.toMillis(1l));
	}

	public void shutdown() {
		registerMBeanTask.cancel();
		if (mbeanManager != null) {
			mbeanManager.unregister();
		}
		executor.shutdownNow();
	}

	@Override
	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}

	@Override
	public int getMaxRetries() {
		return maxRetries;
	}

	@Override
	public void setRetryLimit(int limit) {
		this.retryLimit = limit;
	}

	@Override
	public int getRetryLimit() {
		return retryLimit;
	}

	@Override
	public void setMaxQueueSize(int size) {
		this.maxQueueSize = size;
	}

	@Override
	public int getMaxQueueSize() {
		return maxQueueSize;
	}

	@Override
	public void setQueuePollTimeoutMillis(int millis) {
		this.pollTimeoutMillis = millis;
	}

	@Override
	public int getQueuePollTimeoutMillis() {
		return pollTimeoutMillis;
	}

	@Override
	public void setMinTaskRate(float rate) {
		this.minTaskRate = rate;
	}

	@Override
	public float getMinTaskRate() {
		return minTaskRate;
	}

	@Override
	public float getTaskRatePerSecond() {
		return taskRateTracker.getRatePerSecond();
	}

	@Override
	public TrackingThreadPoolExecutorMXBean getThreadPoolExecutor() {
		return executor;
	}

	/**
     * Asynchronously pulls from an iteration of binding sets and pushes to a {@link BindingSetPipe}.
     * @param pipe the pipe that evaluation results are returned on
     * @param evalStep query step to evaluate
     * @param node an implementation of any {@TupleExpr} sub-type
     * @param bs binding set
     * @param strategy
     */
	void pullAndPushAsync(BindingSetPipe pipe,
			QueryEvaluationStep evalStep,
			TupleExpr node, BindingSet bs, HalyardEvaluationStrategy strategy) {
		executor.execute(new IterateAndPipeTask(pipe, evalStep, node, bs, strategy));
    }

    /**
     * Asynchronously pushes to a pipe using the push action, and returns an iteration of binding sets to pull from.
     * @param evalStep query step to evaluate
     * @param node an implementation of any {@TupleExpr} sub-type
     * @param bs binding set
     * @param strategy
     * @return iteration of binding sets to pull from.
     */
	CloseableIteration<BindingSet, QueryEvaluationException> pushAndPull(BindingSetPipeEvaluationStep evalStep, TupleExpr node, BindingSet bs, HalyardEvaluationStrategy strategy) {
        BindingSetPipeQueue queue = new BindingSetPipeQueue();
        executor.execute(new PipeAndQueueTask(queue.pipe, evalStep, node, bs, strategy));
        return queue.iteration;
	}



	abstract class PrioritizedTask implements Comparable<PrioritizedTask>, Runnable {
    	static final int MIN_SUB_PRIORITY = 0;
    	static final int MAX_SUB_PRIORITY = 99999;
    	final TupleExpr queryNode;
    	final BindingSet bindingSet;
    	final int queryPriority;
    	int taskPriority;

    	PrioritizedTask(TupleExpr queryNode, BindingSet bs, HalyardEvaluationStrategy strategy) {
    		this.queryNode = queryNode;
    		this.bindingSet = bs;
    		this.queryPriority = strategy.execContext.getPriorityForNode(queryNode);
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
    		return super.toString() + "[queryNode = " + printQueryNode(queryNode, bindingSet) + ", priority = " + taskPriority + "]";
    	}
	}

	static String printQueryNode(TupleExpr queryNode, BindingSet bs) {
		final class NodePrinter extends AbstractExtendedQueryModelVisitor<RuntimeException> {
			final StringBuilder sb = new StringBuilder(128);
			@Override
			public void meetNode(QueryModelNode node) {
				sb.append(node.getSignature());
				appendStats(node);
			}
			@Override
			public void meet(StatementPattern node) {
				sb.append(node.getSignature());
				sb.append("(");
				appendVar(node.getSubjectVar());
				sb.append(" ");
				appendVar(node.getPredicateVar());
				sb.append(" ");
				appendVar(node.getObjectVar());
				if (node.getContextVar() != null) {
					sb.append(" ");
					appendVar(node.getContextVar());
				}
				sb.append(")");
				appendStats(node);
			}
			@Override
			public void meet(Service node) {
				sb.append(node.getSignature());
				sb.append("(");
				appendVar(node.getServiceRef());
				sb.append(")");
				appendStats(node);
			}
			void appendVar(Var var) {
				if (!var.isConstant()) {
					sb.append("?").append(var.getName());
				}
				Value v = Algebra.getVarValue(var, bs);
				if (!var.isConstant() && v != null) {
					sb.append("=");
				}
				if (v != null) {
					sb.append(v);
				}
			}
			void appendStats(QueryModelNode node) {
				sb.append("[");
				sb.append("cost = ").append(node.getCostEstimate()).append(", ");
				sb.append("cardinality = ").append(node.getResultSizeEstimate()).append(", ");
				sb.append("count = ").append(node.getResultSizeActual()).append(", ");
				sb.append("time = ").append(node.getTotalTimeNanosActual());
				sb.append("]");
			}
			@Override
			public String toString() {
				return sb.toString();
			}
		}
		NodePrinter nodePrinter = new NodePrinter();
		queryNode.visit(nodePrinter);
		return nodePrinter.toString();
	}

	/**
     * A holder for the BindingSetPipe and the iterator over a tree of query sub-parts
     */
    final class IterateAndPipeTask extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final QueryEvaluationStep evalStep;
    	private final HalyardEvaluationStrategy strategy;
        private int pushPriority = MIN_SUB_PRIORITY;
        private CloseableIteration<BindingSet, QueryEvaluationException> iter;

        /**
         * Constructor for the class with the supplied variables
         * @param pipe The pipe to return evaluations to
         * @param evalStep The query step to evaluation
         */
		IterateAndPipeTask(BindingSetPipe pipe,
				QueryEvaluationStep evalStep,
				TupleExpr expr, BindingSet bs, HalyardEvaluationStrategy strategy) {
			super(expr, bs, strategy);
            this.pipe = pipe;
            this.evalStep = evalStep;
            this.strategy = strategy;
        }

		boolean pushNext() {
        	try {
            	if (!pipe.isClosed()) {
            		if (iter == null) {
                        iter = strategy.track(evalStep.evaluate(bindingSet), queryNode);
            		}
                	if(iter.hasNext()) {
                        BindingSet bs = iter.next();
                        if (pipe.push(bs)) { //true indicates more data is expected from this binding set, put it on the queue
                           	return true;
                        }
            		}
                	pipe.close();
            	}
            	if (iter != null) {
            		iter.close();
            	}
            	return false;
            } catch (Throwable e) {
            	if (iter != null) {
	            	try {
	                    iter.close();
	            	} catch (QueryEvaluationException ignore) {
	            		e.addSuppressed(ignore);
	            	}
            	}
                return pipe.handleException(e);
            }
		}

		@Override
    	public void run() {
        	if (pushNext()) {
        		if (pushPriority < MAX_SUB_PRIORITY) {
        			pushPriority++;
        		}
        		setSubPriority(pushPriority);
                executor.execute(this);
        	}
    	}
    }

    final class PipeAndQueueTask extends PrioritizedTask {
        private final BindingSetPipe pipe;
        private final BindingSetPipeEvaluationStep evalStep;

		PipeAndQueueTask(BindingSetPipe pipe, BindingSetPipeEvaluationStep evalStep, TupleExpr expr, BindingSet bs, HalyardEvaluationStrategy strategy) {
			super(expr, bs, strategy);
			this.pipe = pipe;
			this.evalStep = evalStep;
		}

		@Override
		public void run() {
			try {
				evalStep.evaluate(pipe, bindingSet);
			} catch(Throwable e) {
				pipe.handleException(e);
			}
		}
    }

    final class BindingSetPipeQueue {

        final BindingSetPipeIteration iteration = new BindingSetPipeIteration();
        final QueueingBindingSetPipe pipe = new QueueingBindingSetPipe(maxQueueSize, offerTimeoutMillis, TimeUnit.MILLISECONDS);

        @Override
        public String toString() {
        	return "Pipe "+Integer.toHexString(pipe.hashCode())+" for iteration "+Integer.toHexString(iteration.hashCode());
        }

        /**
         * Used by client to pull data.
         */
        final class BindingSetPipeIteration extends LookAheadIteration<BindingSet, QueryEvaluationException> {
        	@Override
            protected BindingSet getNextElement() throws QueryEvaluationException {
    			Object bs = null;
                for (int retries = 0; bs == null && !isClosed(); retries++) {
            		bs = pipe.poll(pollTimeoutMillis, TimeUnit.MILLISECONDS);
					if (bs == null) {
						// no data available - see if we can improve things
						if (checkThreads(retries)) {
							retries = 0;
						}
					}
                }
                return pipe.isEndOfQueue(bs) ? null : (BindingSet) bs;
            }

        	private boolean checkThreads(int retries) {
        		final boolean overallProgress = taskRateTracker.getRatePerSecond() > minTaskRate;
        		// if not making any progress overall
        		if (!overallProgress) {
            		final int maxPoolSize = executor.getMaximumPoolSize();
	        		// if we've been consistently blocked and are at full capacity
	        		if (retries > maxRetries && executor.getActiveCount() >= maxPoolSize) {
	        			// then try adding some emergency threads
	    				synchronized (executor) {
	    					// check thread pool hasn't been modified already in the meantime and still blocked
	    					if (maxPoolSize == executor.getMaximumPoolSize() && executor.getActiveCount() >= maxPoolSize && taskRateTracker.getRatePerSecond() <= minTaskRate) {
	    						if (maxPoolSize < maxThreads) {
	    							int newMaxPoolSize = Math.min(maxPoolSize + threadGain, maxThreads);
	    							LOGGER.warn("Iteration {}: all {} threads seem to be blocked (taskRate {}) - adding {} more", Integer.toHexString(this.hashCode()), executor.getPoolSize(), taskRateTracker.getRatePerSecond(), newMaxPoolSize - maxPoolSize);
	    							executor.setMaximumPoolSize(newMaxPoolSize);
	    							executor.setCorePoolSize(Math.min(executor.getCorePoolSize()+threadGain, newMaxPoolSize));
	    						} else {
	    							// out of options
	    							throw new QueryEvaluationException(String.format("Maximum thread limit reached (%d)", maxThreads));
	    						}
	    					}
	    				}
						return true;
	        		} else if (retries > retryLimit) {
	        			// something else is wrong
	        			throw new QueryEvaluationException(String.format("Retry limit exceeded: %d (active threads %d, task rate %f)", retries, executor.getActiveCount(), taskRateTracker.getRatePerSecond()));
	        		}
        		}
        		return overallProgress;
            }

            @Override
            protected void handleClose() throws QueryEvaluationException {
                super.handleClose();
               	pipe.close();
            }

            @Override
            public String toString() {
            	return "Iteration "+Integer.toHexString(this.hashCode())+" for pipe "+Integer.toHexString(pipe.hashCode());
            }
        }
    }
}
