package com.msd.gin.halyard.query;

public final class AbortConsumerException extends RuntimeException {
	private static final long serialVersionUID = -2745490675867153819L;
	private static final AbortConsumerException INSTANCE = new AbortConsumerException();

	private AbortConsumerException() {
		super(null, null, false, false);
	}

	public static void abort() {
		throw INSTANCE;
	}
}
