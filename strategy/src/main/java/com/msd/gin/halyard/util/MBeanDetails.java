package com.msd.gin.halyard.util;

import com.msd.gin.halyard.strategy.StrategyConfig;

import java.util.Hashtable;
import java.util.Map;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public final class MBeanDetails {
	private final Object o;
	private final Class<?> intf;
	private final ObjectName name;

	public MBeanDetails(Object o, Class<?> intf, Map<String,String> customAttrs) {
		this.o = o;
		this.intf = intf;
		Hashtable<String,String> attrs = new Hashtable<>(customAttrs);
		attrs.put("type", o.getClass().getName());
		attrs.put("id", MBeanManager.getId(o));
		try {
			name = ObjectName.getInstance(StrategyConfig.JMX_DOMAIN, attrs);
		} catch (MalformedObjectNameException e) {
			throw new AssertionError(e);
		}
	}

	public Object getMBean() {
		return o;
	}

	public Class<?> getInterface() {
		return intf;
	}

	public ObjectName getName() {
		return name;
	}
}