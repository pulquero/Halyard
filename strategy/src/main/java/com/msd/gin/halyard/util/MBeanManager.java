package com.msd.gin.halyard.util;

import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.management.JMException;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

public abstract class MBeanManager<T> {
	private final MBeanServer mbs;
	private List<ObjectInstance> mbeanInsts;

	public static String getId(Object o) {
		return o.getClass().getSimpleName() + "@" + Integer.toHexString(o.hashCode());
	}

	public MBeanManager() {
		this.mbs = ManagementFactory.getPlatformMBeanServer();
	}

	public final void register(T t) {
		List<MBeanDetails> mbeanObjs = mbeans(t);
		mbeanInsts = new ArrayList<>(mbeanObjs.size());
		try {
			for (MBeanDetails mbeanObj : mbeanObjs) {
				Class<?> intf = mbeanObj.getInterface();
				Object weakMBean = Proxy.newProxyInstance(intf.getClassLoader(), new Class[] {intf, MBeanRegistration.class}, new WeakMBean(mbeanObj.getMBean()));
				mbeanInsts.add(mbs.registerMBean(weakMBean, mbeanObj.getName()));
			}
		} catch (JMException e) {
			throw new AssertionError(e);
		}
	}

	protected abstract List<MBeanDetails> mbeans(T t);

	public final void unregister() {
		for (Iterator<ObjectInstance> iter = mbeanInsts.iterator(); iter.hasNext(); ) {
			ObjectInstance inst = iter.next();
			try {
				mbs.unregisterMBean(inst.getObjectName());
			} catch (JMException e) {
				// ignore
			}
			iter.remove();
		}
	}


	final class WeakMBean implements InvocationHandler, MBeanRegistration {
		private final WeakReference<Object> mbeanRef;
		private ObjectName name;

		WeakMBean(Object mbean) {
			mbeanRef = new WeakReference<>(mbean);
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
				return method.invoke(mbean, args);
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
