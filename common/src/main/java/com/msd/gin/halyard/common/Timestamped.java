package com.msd.gin.halyard.common;

public interface Timestamped {
	static final long NOT_SET = -1L;

	long getTimestamp();
	void setTimestamp(long ts);
}
