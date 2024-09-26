package com.msd.gin.halyard.sail;

import java.util.Map;

public interface QueryHelperProvider {
	Class<?> getQueryHelperClass();

	Object createQueryHelper(Map<String, String> config);
}
