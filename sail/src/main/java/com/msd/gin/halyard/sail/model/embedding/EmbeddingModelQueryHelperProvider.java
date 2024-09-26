package com.msd.gin.halyard.sail.model.embedding;

import com.msd.gin.halyard.sail.QueryHelperProvider;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;

import org.kohsuke.MetaInfServices;

import dev.langchain4j.model.embedding.EmbeddingModel;

@MetaInfServices(QueryHelperProvider.class)
public class EmbeddingModelQueryHelperProvider implements QueryHelperProvider<EmbeddingModel> {

	@Override
	public Class<EmbeddingModel> getQueryHelperClass() {
		return EmbeddingModel.class;
	}

	@Override
	public EmbeddingModel createQueryHelper(Map<String, String> config) throws Exception {
		String modelClassName = config.get("model.class");
		Class<?> modelClass = Class.forName(modelClassName);
		Method builderMethod;
		try {
			builderMethod = modelClass.getMethod("builder");
		} catch (NoSuchMethodException ex) {
			builderMethod = null;
		}
		if (builderMethod != null) {
			Object builder = builderMethod.invoke(null);
			Class<?> builderClass = builder.getClass();
			Method buildMethod = builderClass.getMethod("build");
			for (Method m : builderClass.getMethods()) {
				if (m.getParameterCount() == 1) {
					String key = m.getName();
					String value = config.get(key);
					if (value != null) {
						m.invoke(builder, convert(value, m.getParameterTypes()[0]));
					}
				}
			}
			return (EmbeddingModel) buildMethod.invoke(builder);
		} else {
			return (EmbeddingModel) modelClass.getConstructor().newInstance();
		}
	}

	private static Object convert(String value, Class<?> targetType) {
		if (targetType == String.class) {
			return value;
		} else if (targetType == Integer.class) {
			return Integer.parseInt(value);
		} else if (targetType == Boolean.class) {
			return Boolean.parseBoolean(value);
		} else if (targetType == Duration.class) {
			if (value.endsWith("ms")) {
				return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - "ms".length())));
			} else if (value.endsWith("s")) {
				return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - "s".length())));
			} else if (value.endsWith("min")) {
				return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - "min".length())));
			} else {
				throw new IllegalArgumentException(String.format("Unsupported duration value: %s", value));
			}
		} else {
			throw new IllegalArgumentException(String.format("Unsupported type: %s", targetType.getName()));
		}
	}
}
