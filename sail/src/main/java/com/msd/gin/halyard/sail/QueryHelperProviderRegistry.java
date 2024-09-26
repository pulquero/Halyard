package com.msd.gin.halyard.sail;

import java.util.Objects;

import org.eclipse.rdf4j.common.lang.service.ServiceRegistry;

public class QueryHelperProviderRegistry extends ServiceRegistry<String, QueryHelperProvider> {
	private final static QueryHelperProviderRegistry defaultRegistry = new QueryHelperProviderRegistry();

	public static QueryHelperProviderRegistry getInstance() {
		return defaultRegistry;
	}

	public QueryHelperProviderRegistry() {
		super(QueryHelperProvider.class);
	}

	@Override
	protected String getKey(QueryHelperProvider service) {
		return Objects.requireNonNull(service.getQueryHelperClass().getCanonicalName(), "Query helper class must have a canonical name");
	}
}
