package com.msd.gin.halyard.model;

public interface Wrapper<T> {
	T unwrap();

	static <T> T unwrap(T o) {
		if (o instanceof Wrapper<?>) {
			return ((Wrapper<T>)o).unwrap();
		} else {
			return o;
		}
	}
}
