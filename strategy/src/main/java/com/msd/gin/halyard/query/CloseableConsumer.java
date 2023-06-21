package com.msd.gin.halyard.query;

import java.util.function.Consumer;

public interface CloseableConsumer<T> extends AutoCloseable, Consumer<T> {
	static <T> CloseableConsumer<T> wrap(Consumer<T> consumer) {
		return new CloseableConsumer<T>() {
			@Override
			public void accept(T t) {
				consumer.accept(t);
			}

			@Override
			public void close() {
			}
		};
	}

	@Override
	void close();
}
