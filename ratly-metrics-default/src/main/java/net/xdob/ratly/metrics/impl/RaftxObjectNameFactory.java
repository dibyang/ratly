
package net.xdob.ratly.metrics.impl;

import com.codahale.metrics.jmx.ObjectNameFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

class RatlyObjectNameFactory implements ObjectNameFactory {
  @Override
  public ObjectName createName(String type, String domain, String name) {
    try {
      ObjectName objectName = new ObjectName(domain, "name", name);
      if (objectName.isPattern()) {
        objectName = new ObjectName(domain, "name", ObjectName.quote(name));
      }
      return objectName;
    } catch (MalformedObjectNameException e) {
      try {
        return new ObjectName(domain, "name", ObjectName.quote(name));
      } catch (MalformedObjectNameException mone) {
        throw new IllegalArgumentException(
            "Failed to register " + name + ", type=" + type + ", domain=" + domain, mone);
      }
    }
  }
}
