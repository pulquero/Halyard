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

import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.QueueingBindingSetPipe;
import com.msd.gin.halyard.query.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.util.MBeanDetails;
import com.msd.gin.halyard.util.MBeanManager;
import com.msd.gin.halyard.util.RateTracker;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

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

public final class HalyardEvaluationExecutor implements HalyardEvaluationExecutorMXBean {
    private static final Timer TIMER = new Timer("HalyardEvaluationExecutorTimer", true);

    private final AtomicLong incomingBindingsCount = new AtomicLong();
    private final AtomicLong outgoingBindingsCount = new AtomicLong();
    private RateTracker incomingBindingsRateTracker;
    private RateTracker outgoingBindingsRateTracker;

	private int maxQueueSize;
	private int pollTimeoutMillis;
	private int offerTimeoutMillis;
	private final PullPusher pullPusher;

	private final TimerTask registerMBeanTask;
	private MBeanManager<HalyardEvaluationExecutor> mbeanManager;

	public static HalyardEvaluationExecutor create(String name, Configuration conf, boolean asyncPullPush, Map<String,String> connAttrs) {
		return new HalyardEvaluationExecutor(name, conf, asyncPullPush, connAttrs);
	}

	private HalyardEvaluationExecutor(String name, Configuration conf, boolean asyncPullPush, Map<String,String> connAttrs) {
	    maxQueueSize = conf.getInt(StrategyConfig.HALYARD_EVALUATION_MAX_QUEUE_SIZE, StrategyConfig.DEFAULT_MAX_QUEUE_SIZE);
		pollTimeoutMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_POLL_TIMEOUT_MILLIS, Integer.MAX_VALUE);
		offerTimeoutMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_OFFER_TIMEOUT_MILLIS, conf.getInt("hbase.client.scanner.timeout.period", 60000));

		if (asyncPullPush) {
			pullPusher = new AsyncPullPusher(name, conf);
		} else {
			pullPusher = new SyncPullPusher();
		}

		int bindingsRateUpdateMillis = conf.getInt(StrategyConfig.HALYARD_EVALUATION_BINDINGS_RATE_UPDATE_MILLIS, 100);
		int bindingsRateWindowSize = conf.getInt(StrategyConfig.HALYARD_EVALUATION_BINDINGS_RATE_WINDOW_SIZE, 10);
		if (bindingsRateWindowSize > 0) {
			incomingBindingsRateTracker = new RateTracker(TIMER, bindingsRateUpdateMillis, bindingsRateWindowSize, () -> incomingBindingsCount.get());
			incomingBindingsRateTracker.start();
			outgoingBindingsRateTracker = new RateTracker(TIMER, bindingsRateUpdateMillis, bindingsRateWindowSize, () -> outgoingBindingsCount.get());
			outgoingBindingsRateTracker.start();
		}

		// don't both registering MBeans for short-lived queries
		registerMBeanTask = new TimerTask() {
			@Override
			public void run() {
				mbeanManager = new MBeanManager<>() {
					@Override
					protected List<MBeanDetails> mbeans(HalyardEvaluationExecutor executor) {
						List<MBeanDetails> mbeanObjs = new ArrayList<>(2);
						mbeanObjs.add(new MBeanDetails(executor, HalyardEvaluationExecutorMXBean.class, connAttrs));
						if (executor.getThreadPoolExecutor() != null) {
							mbeanObjs.add(new MBeanDetails(executor.getThreadPoolExecutor(), TrackingThreadPoolExecutorMXBean.class, connAttrs));
						}
						return mbeanObjs;
					}
				};
				mbeanManager.register(HalyardEvaluationExecutor.this);
			}
		};
		TIMER.schedule(registerMBeanTask, TimeUnit.MINUTES.toMillis(1l));
	}

	HalyardEvaluationExecutor(Configuration conf) {
		this("Halyard", conf, true, Collections.emptyMap());
	}

	public void shutdown() {
		if (incomingBindingsRateTracker != null) {
			incomingBindingsRateTracker.stop();
		}
		if (outgoingBindingsRateTracker != null) {
			outgoingBindingsRateTracker.stop();
		}
		registerMBeanTask.cancel();
		if (mbeanManager != null) {
			mbeanManager.unregister();
		}
		pullPusher.close();
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
	public float getIncomingBindingsRatePerSecond() {
		return (incomingBindingsRateTracker != null) ? incomingBindingsRateTracker.getRatePerSecond() : Float.NaN;
	}

	@Override
	public float getOutgoingBindingsRatePerSecond() {
		return (outgoingBindingsRateTracker != null) ? outgoingBindingsRateTracker.getRatePerSecond() : Float.NaN;
	}

	@Override
	public void setAsyncPullPushAllLimit(int limit) {
		if (pullPusher instanceof AsyncPullPusher) {
			((AsyncPullPusher) pullPusher).setPullPushAllLimit(limit);
		}
	}

	/**
	 * @return -1 if infinite
	 */
	@Override
	public int getAsyncPullPushAllLimit() {
		return (pullPusher instanceof AsyncPullPusher) ? ((AsyncPullPusher) pullPusher).getPullPushAllLimit() : 0;
	}

	@Override
	public void setAsyncTaskQueueMaxSize(int size) {
		if (pullPusher instanceof AsyncPullPusher) {
			((AsyncPullPusher) pullPusher).setTaskQueueMaxSize(size);
		}
	}

	@Override
	public int getAsyncTaskQueueMaxSize() {
		return (pullPusher instanceof AsyncPullPusher) ? ((AsyncPullPusher) pullPusher).getTaskQueueMaxSize() : 0;
	}

	@Override
	public TrackingThreadPoolExecutorMXBean getThreadPoolExecutor() {
		return (pullPusher instanceof AsyncPullPusher) ? ((AsyncPullPusher) pullPusher).getThreadPoolExecutorMXBean() : null;
	}

	/**
     * Pulls from an iteration of binding sets and pushes to a {@link BindingSetPipe}.
     * @param pipe the pipe that evaluation results are returned on
     * @param evalStep query step to evaluate
     * @param node an implementation of any {@TupleExpr} sub-type
     * @param bs binding set
     * @param trackerFactory
     */
	void pullPushAsync(BindingSetPipe pipe,
			QueryEvaluationStep evalStep,
			TupleExpr node, BindingSet bs,
			Function<CloseableIteration<BindingSet>,CloseableIteration<BindingSet>> trackerFactory) {
		BindingSetPipe childPipe = new CountingBindingSetPipe(pipe, incomingBindingsCount);
		pullPusher.pullPush(childPipe, evalStep, node, bs, trackerFactory);
    }

    /**
     * Asynchronously pushes to a pipe using the push action, and returns an iteration of binding sets to pull from.
     * @param evalStep query step to evaluate
     * @param node an implementation of any {@TupleExpr} sub-type
     * @param bs binding set
     * @return iteration of binding sets to pull from.
     */
	CloseableIteration<BindingSet> pushPullAsync(BindingSetPipeEvaluationStep evalStep, TupleExpr node, BindingSet bs) {
        QueueingBindingSetPipe pipe = new QueueingBindingSetPipe(maxQueueSize, offerTimeoutMillis, TimeUnit.MILLISECONDS);
        BindingSetPipe childPipe = new CountingBindingSetPipe(pipe, outgoingBindingsCount);
        Thread thr = new Thread(new PipeAndQueueTask(childPipe, evalStep, bs));
        thr.setDaemon(true);
        thr.start();
        return new BindingSetPipeIteration(pipe);
	}

	static final class PipeAndQueueTask implements Runnable {
    	private final BindingSet bindingSet;
        private final BindingSetPipe pipe;
        private final BindingSetPipeEvaluationStep evalStep;

		PipeAndQueueTask(BindingSetPipe pipe, BindingSetPipeEvaluationStep evalStep, BindingSet bs) {
			this.bindingSet = bs;
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

	void pushPullSync(Consumer<BindingSet> handler, BindingSetPipeEvaluationStep evalStep, BindingSet bindings) {
		QueueingBindingSetPipe pipe = new QueueingBindingSetPipe(maxQueueSize, offerTimeoutMillis, TimeUnit.MILLISECONDS);
		evalStep.evaluate(new CountingBindingSetPipe(pipe, outgoingBindingsCount), bindings);
		pipe.collect(handler, pollTimeoutMillis, TimeUnit.MILLISECONDS);
	}

	void push(BindingSetPipe pipe, BindingSetPipeEvaluationStep evalStep, BindingSet bindings) {
		evalStep.evaluate(new CountingBindingSetPipe(pipe, outgoingBindingsCount), bindings);
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
     * Used by client to pull data.
     */
    final class BindingSetPipeIteration extends LookAheadIteration<BindingSet> {
    	final Deque<BindingSet> lookAheadBuffer = new ArrayDeque<>(maxQueueSize);
    	final QueueingBindingSetPipe pipe;
    	boolean hasMore = true;

    	BindingSetPipeIteration(QueueingBindingSetPipe pipe) {
    		this.pipe = pipe;
    	}

		@Override
		protected BindingSet getNextElement() throws QueryEvaluationException {
			if (lookAheadBuffer.isEmpty() && hasMore) {
				hasMore = pipe.pollThenElse(lookAheadBuffer::add, () -> {
					throw new QueryEvaluationException(String.format("Exceeded poll time-out of %dms (active threads %d, queue size %d, incoming binding set rate %f)", pollTimeoutMillis, pullPusher.getActiveCount(), pullPusher.getQueueSize(), incomingBindingsRateTracker.getRatePerSecond()));
				}, pollTimeoutMillis, TimeUnit.MILLISECONDS);
			}
			return lookAheadBuffer.poll();
		}

        @Override
        protected void handleClose() throws QueryEvaluationException {
            pipe.close();
        }

        @Override
        public String toString() {
        	return "Iteration "+Integer.toHexString(this.hashCode())+" for pipe "+Integer.toHexString(pipe.hashCode());
        }
    }


    static final class CountingBindingSetPipe extends BindingSetPipe {
    	private final AtomicLong counter;

    	protected CountingBindingSetPipe(BindingSetPipe parent, AtomicLong counter) {
			super(parent);
			this.counter = counter;
		}

		@Override
		public boolean next(BindingSet bs) {
			counter.incrementAndGet();
			return parent.push(bs);
		}
    }
}

