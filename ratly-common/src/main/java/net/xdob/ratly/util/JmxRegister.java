
package net.xdob.ratly.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.function.Supplier;

/** For registering JMX beans. */
public class JmxRegister {
  static final Logger LOG = LoggerFactory.getLogger(JmxRegister.class);

  private ObjectName registeredName;

  static ObjectName tryRegister(String name, Object mBean) {
    final ObjectName objectName;
    try {
      objectName = new ObjectName(name);
      ManagementFactory.getPlatformMBeanServer().registerMBean(mBean, objectName);
    } catch (Exception e) {
      LOG.error("Failed to register JMX Bean with name " + name, e);
      return null;
    }

    LOG.info("Successfully registered JMX Bean with object name " + objectName);
    return objectName;
  }

  /**
   * Try registering the mxBean with the names one by one.
   * @return the registered name, or, if it fails, return null.
   */
  public synchronized String register(Object mxBean, Iterable<Supplier<String>> names) {
    if (registeredName == null) {
      for (Supplier<String> supplier : names) {
        final String name = supplier.get();
        registeredName = tryRegister(name, mxBean);
        if (registeredName != null) {
          LOG.info("register mxBean {} as {}", mxBean.getClass(), name);
          return name;
        }
      }
    }

    // failed
    return null;
  }

  /** Un-register the previously registered mBean. */
  public synchronized boolean unregister() throws JMException {
    if (registeredName == null) {
      return false;
    }
    ManagementFactory.getPlatformMBeanServer().unregisterMBean(registeredName);
    LOG.info("Successfully un-registered JMX Bean with object name " + registeredName);
    registeredName = null;
    return true;
  }
}
