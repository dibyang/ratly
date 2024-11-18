package net.xdob.ratly.datastream;

import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.ReflectionUtils;

/** The type of data stream implementations. */
public interface DataStreamType {
  /**
   * Parse the given string as a {@link SupportedDataStreamType}
   * or a user-defined {@link DataStreamType}.
   *
   * @param dataStreamType The string representation of an {@link DataStreamType}.
   * @return a {@link SupportedDataStreamType} or a user-defined {@link DataStreamType}.
   */
  static DataStreamType valueOf(String dataStreamType) {
    final Throwable fromSupportedRpcType;
    try { // Try parsing it as a SupportedRpcType
      return SupportedDataStreamType.valueOfIgnoreCase(dataStreamType);
    } catch (Throwable t) {
      fromSupportedRpcType = t;
    }

    try {
      // Try using it as a class name
      return ReflectionUtils.newInstance(ReflectionUtils.getClass(dataStreamType, DataStreamType.class));
    } catch(Throwable t) {
      final String classname = JavaUtils.getClassSimpleName(DataStreamType.class);
      final IllegalArgumentException iae = new IllegalArgumentException(
          "Invalid " + classname + ": \"" + dataStreamType + "\" "
              + " cannot be used as a user-defined " + classname
              + " and it is not a " + JavaUtils.getClassSimpleName(SupportedDataStreamType.class) + ".");
      iae.addSuppressed(t);
      iae.addSuppressed(fromSupportedRpcType);
      throw iae;
    }
  }

  /** @return the name of the rpc type. */
  String name();

  /** @return a new client factory created using the given parameters. */
  DataStreamFactory newClientFactory(Parameters parameters);

  /** @return a new server factory created using the given parameters. */
  DataStreamFactory newServerFactory(Parameters parameters);

  /** An interface to get {@link DataStreamType}. */
  interface Get {
    /** @return the {@link DataStreamType}. */
    DataStreamType getDataStreamType();
  }
}