package com.msd.gin.halyard.util;

import java.util.NoSuchElementException;
import java.util.Objects;

public final class TriOptional<T> {
	private static final Object _INVALID = new Object();
	private static final TriOptional<?> EMPTY = new TriOptional<>(null);
	private static final TriOptional<?> INVALID = new TriOptional<>(_INVALID);

	public static <T> TriOptional<T> of(T e) {
		return new TriOptional<>(Objects.requireNonNull(e));
	}

	public static <T> TriOptional<T> ofNullable(T e) {
		return (e != null) ? new TriOptional<>(e) : empty();
	}

	public static <T> TriOptional<T> empty() {
		return (TriOptional<T>) EMPTY;
	}

	public static <T> TriOptional<T> invalid() {
		return (TriOptional<T>) INVALID;
	}

	private final T value;

	private TriOptional(T e) {
		this.value = e;
	}

	public T get() {
		if (value == null || value == _INVALID) {
			throw new NoSuchElementException();
		}
		return value;
	}

	public boolean isEmpty() {
		return value == null;
	}

	public boolean isInvalid() {
		return value == _INVALID;
	}

	public T orElse(T other) {
		return (value != null) ? value : other;
	}
}
