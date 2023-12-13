package com.msd.gin.halyard.util;

import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.management.JMException;
import javax.management.JMX;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.commons.lang3.tuple.Pair;

public abstract class MBeanManager<T> {
	private final MBeanServer mbs;
	private Map<Object,Pair<ObjectInstance,Object>> wrappers;

	public static String getId(Object o) {
		return Integer.toHexString(o.hashCode());
	}

	public MBeanManager() {
		this.mbs = ManagementFactory.getPlatformMBeanServer();
	}

	public final void register(T t) {
		List<MBeanDetails> mbeanObjs = mbeans(t);
		wrappers = new IdentityHashMap<>(mbeanObjs.size() + 1);
		try {
			for (MBeanDetails mbeanObj : mbeanObjs) {
				Class<?> intf = mbeanObj.getInterface();
				Object weakMBean = Proxy.newProxyInstance(intf.getClassLoader(), new Class[] {intf, MBeanRegistration.class}, new WeakMBean(mbeanObj));
				ObjectInstance inst = mbs.registerMBean(weakMBean, mbeanObj.getName());
				wrappers.put(mbeanObj.getMBean(), Pair.of(inst, weakMBean));
			}
		} catch (JMException e) {
			throw new AssertionError(e);
		}
	}

	protected abstract List<MBeanDetails> mbeans(T t);

	public final void unregister() {
		for (Iterator<Pair<ObjectInstance,Object>> iter = wrappers.values().iterator(); iter.hasNext(); ) {
			ObjectInstance inst = iter.next().getKey();
			try {
				mbs.unregisterMBean(inst.getObjectName());
			} catch (JMException e) {
				// ignore
			}
			iter.remove();
		}
	}


	final class WeakMBean extends MBeanServerInvocationHandler implements MBeanRegistration {
		private final WeakReference<Object> mbeanRef;
		private ObjectName name;

		WeakMBean(MBeanDetails mbeanObj) {
			super(mbs, mbeanObj.getName(), mbeanObj.isMXBean());
			mbeanRef = new WeakReference<>(mbeanObj.getMBean());
		}

		@Override
		public ObjectName getObjectName() {
			return name;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			Object mbean = mbeanRef.get();
			if (mbean == null) {
				unregister();
				throw new IllegalStateException("MBean is no longer available and should have been unregistered: "+name);
			}
			if (method.getDeclaringClass() == MBeanRegistration.class) {
				return method.invoke(this, args);
			} else {
				Object result = method.invoke(mbean, args);
				if (JMX.isMXBeanInterface(method.getReturnType())) {
					// result maybe proxied
					Pair<ObjectInstance,Object> wrapper = wrappers.get(result);
					if (wrapper != null) {
						return wrapper.getValue();
					}
				}
				return result;
			}
		}

		void unregister() {
			if (name != null) {
				try {
					mbs.unregisterMBean(name);
				} catch (JMException e) {
					// ignore
				}
			}
		}

		@Override
		public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
			this.name = name;
			return name;
		}

		@Override
		public void postRegister(Boolean registrationDone) {
		}

		@Override
		public void preDeregister() throws Exception {
		}

		@Override
		public void postDeregister() {
		}
	}
}
