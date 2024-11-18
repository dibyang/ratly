

package net.xdob.ratly.conf;

import static net.xdob.ratly.conf.ReconfigurationStatus.propertyString;

public class ReconfigurationException extends Exception {
  private static final long serialVersionUID = 1L;

  private final String property;
  private final String newValue;
  private final String oldValue;

  /**
   * Create a new instance of {@link ReconfigurationException}.
   * @param property the property name.
   * @param newValue the new value.
   * @param oldValue the old value.
   * @param cause the cause of this exception.
   */
  public ReconfigurationException(String reason, String property, String newValue, String oldValue, Throwable cause) {
    super("Failed to change property " + propertyString(property, newValue, oldValue) + ": " + reason, cause);
    this.property = property;
    this.newValue = newValue;
    this.oldValue = oldValue;
  }

  /** The same as new ReconfigurationException(reason, property, newValue, oldValue, null). */
  public ReconfigurationException(String reason, String property, String newValue, String oldValue) {
    this(reason, property, newValue, oldValue, null);
  }

  /** @return the property name related to this exception. */
  public String getProperty() {
    return property;
  }

  /** @return the value that the property was supposed to be changed. */
  public String getNewValue() {
    return newValue;
  }

  /** @return the old value of the property. */
  public String getOldValue() {
    return oldValue;
  }
}
