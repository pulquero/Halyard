package com.msd.gin.halyard.query;

import java.lang.ref.WeakReference;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TimeLimitConsumer<E> implements Consumer<E> {
	private static final Logger LOGGER = LoggerFactory.getLogger(TimeLimitConsumer.class);
	private static final Timer timer = new Timer("TimeLimitConsumer", true);

	public static <E> Consumer<E> apply(Consumer<E> handler, int timeLimitSecs) {
		if (timeLimitSecs > 0) {
			handler = new TimeLimitConsumer<>(handler, TimeUnit.SECONDS.toMillis(timeLimitSecs));
		}
		return handler;
	}

	private final Consumer<E> delegate;
	private final long timeLimitMillis;
	private volatile boolean isInterrupted;

	public TimeLimitConsumer(Consumer<E> handler, long timeLimitMillis) {
		assert timeLimitMillis > 0 : "time limit must be a positive number, is: " + timeLimitMillis;
		this.delegate = handler;
		this.timeLimitMillis = timeLimitMillis;
		timer.schedule(new InterruptTask(this), timeLimitMillis);
	}

	@Override
	public void accept(E e) {
		if (isInterrupted) {
			throw new QueryInterruptedException(String.format("Query evaluation exceeded specified timeout %ds", TimeUnit.MILLISECONDS.toSeconds(timeLimitMillis)));
		}
		delegate.accept(e);
	}

	private void interrupt() {
		isInterrupted = true;
	}


	private static final class InterruptTask extends TimerTask {
		private final WeakReference<TimeLimitConsumer<?>> handlerRef;

		private InterruptTask(TimeLimitConsumer<?> handler) {
			handlerRef = new WeakReference<>(handler);
		}

		@Override
		public void run() {
			TimeLimitConsumer<?> handler = handlerRef.get();
			if (handler != null) {
				LOGGER.info("Interrupting - query evaluation exceeded specified timeout {}s", TimeUnit.MILLISECONDS.toSeconds(handler.timeLimitMillis));
				handler.interrupt();
			}
		}
	}
}
