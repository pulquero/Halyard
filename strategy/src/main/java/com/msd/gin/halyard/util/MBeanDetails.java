package com.msd.gin.halyard.util;

import com.msd.gin.halyard.strategy.StrategyConfig;

import java.util.Map;

import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public final class MBeanDetails {
	private final Object o;
	private final Class<?> intf;
	private final ObjectName name;

	public MBeanDetails(Object o, Class<?> intf, Map<String,String> customAttrs) {
		this.o = o;
		this.intf = intf;
		StringBuilder buf = new StringBuilder();
		buf.append(StrategyConfig.JMX_DOMAIN);
		buf.append(":");
		buf.append("type=").append(o.getClass().getName());
		buf.append(",");
		buf.append("id=").append(MBeanManager.getId(o));
		for (Map.Entry<String,String> attr : customAttrs.entrySet()) {
			buf.append(",");
			buf.append(attr.getKey()).append("=").append(attr.getValue());
		}
		try {
			name = ObjectName.getInstance(buf.toString());
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

	public boolean isMXBean() {
		return JMX.isMXBeanInterface(intf);
	}

	public ObjectName getName() {
		return name;
	}
}