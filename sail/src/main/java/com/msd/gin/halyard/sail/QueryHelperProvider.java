package com.msd.gin.halyard.sail;

import java.util.Map;

public interface QueryHelperProvider<T> {
	Class<T> getQueryHelperClass();

	T createQueryHelper(Map<String, String> config) throws Exception;
}
