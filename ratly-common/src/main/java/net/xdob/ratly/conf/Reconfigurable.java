

package net.xdob.ratly.conf;

import java.util.Collection;

/**
 * To reconfigure {@link RaftProperties} in runtime.
 */
public interface Reconfigurable {
  /** @return the {@link RaftProperties} to be reconfigured. */
  RaftProperties getProperties();

  /**
   * Change a property on this object to the new value specified.
   * If the new value specified is null, reset the property to its default value.
   * <p>
   * This method must apply the change to all internal data structures derived
   * from the configuration property that is being changed.
   * If this object owns other {@link Reconfigurable} objects,
   * it must call this method recursively in order to update all these objects.
   *
   * @param property the name of the given property.
   * @param newValue the new value.
   * @return the effective value, which could possibly be different from specified new value,
   *         of the property after reconfiguration.
   * @throws ReconfigurationException if the property is not reconfigurable or there is an error applying the new value.
   */
  String reconfigureProperty(String property, String newValue) throws ReconfigurationException;

  /**
   * Is the given property reconfigurable at runtime?
   *
   * @param property the name of the given property.
   * @return true iff the given property is reconfigurable.
   */
  default boolean isPropertyReconfigurable(String property) {
    return getReconfigurableProperties().contains(property);
  }

  /** @return all the properties that are reconfigurable at runtime. */
  Collection<String> getReconfigurableProperties();
}
