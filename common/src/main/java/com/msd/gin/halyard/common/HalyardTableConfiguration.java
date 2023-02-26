package com.msd.gin.halyard.common;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Persistable configuration only.
 * Immutable.
 */
final class HalyardTableConfiguration {
	private final Map<String,String> config = new HashMap<>();
	private final int hashCode;

	private static void copyTableConfig(Configuration conf, Map<String,String> map) {
		for (Map.Entry<String, String> entry : conf) {
			String prop = entry.getKey();
			if (TableConfig.contains(prop)) {
				map.put(prop, entry.getValue());
			}
		}
	}

	HalyardTableConfiguration(Configuration conf) {
		Configuration defaultConf = new Configuration(false);
		defaultConf.addResource(TableConfig.class.getResource("default-config.xml"));
		copyTableConfig(defaultConf, config);
		copyTableConfig(conf, config);
		hashCode = config.hashCode();
	}

	String get(String name) {
		return config.get(name);
	}

	boolean getBoolean(String name) {
		return Boolean.parseBoolean(config.get(name));
	}

	int getInt(String name) {
		return Integer.parseInt(config.get(name));
	}

	int getInt(String name, int defaultValue) {
		String value = config.get(name);
		return (value != null) ? Integer.parseInt(value) : defaultValue;
	}

	void writeXml(OutputStream out) throws IOException {
		Configuration conf = new Configuration(false);
		conf.addResource(TableConfig.class.getResource("default-config.xml"));
		for (Map.Entry<String, String> entry : config.entrySet()) {
			conf.set(entry.getKey(), entry.getValue());
		}
		conf.writeXml(out);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other == null || this.getClass() != other.getClass()) {
			return false;
		}
		HalyardTableConfiguration that = (HalyardTableConfiguration) other;
		return this.config.equals(that.config);
	}
}
