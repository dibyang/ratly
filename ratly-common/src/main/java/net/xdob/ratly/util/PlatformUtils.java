

package net.xdob.ratly.util;

/**
 * Platform and architecture related utility methods.
 */
public final class PlatformUtils {

  private PlatformUtils() {
    // Utility class, cannot instantiate
  }

  public static final boolean PPC_64
      = System.getProperties().getProperty("os.arch").contains("ppc64");
  /**
   * Get the type of the operating system, as determined from parsing
   * the <code>os.name</code> property.
   */
  private static final OSType OS_TYPE = getOSType();
  public static final boolean OTHER   = (OS_TYPE == OSType.OS_TYPE_OTHER);
  public static final boolean LINUX   = (OS_TYPE == OSType.OS_TYPE_LINUX);
  public static final boolean FREEBSD = (OS_TYPE == OSType.OS_TYPE_FREEBSD);
  public static final boolean MAC     = (OS_TYPE == OSType.OS_TYPE_MAC);
  public static final boolean SOLARIS = (OS_TYPE == OSType.OS_TYPE_SOLARIS);
  // Helper static vars for each platform
  public static final boolean WINDOWS = (OS_TYPE == OSType.OS_TYPE_WIN);

  private static OSType getOSType() {
    String osName = System.getProperty("os.name");
    if (osName.startsWith("Windows")) {
      return OSType.OS_TYPE_WIN;
    } else if (osName.contains("SunOS") || osName.contains("Solaris")) {
      return OSType.OS_TYPE_SOLARIS;
    } else if (osName.contains("Mac")) {
      return OSType.OS_TYPE_MAC;
    } else if (osName.contains("FreeBSD")) {
      return OSType.OS_TYPE_FREEBSD;
    } else if (osName.startsWith("Linux")) {
      return OSType.OS_TYPE_LINUX;
    } else {
      // Some other form of Unix
      return OSType.OS_TYPE_OTHER;
    }
  }

  // OSType detection
  public enum OSType {
    OS_TYPE_LINUX,
    OS_TYPE_WIN,
    OS_TYPE_SOLARIS,
    OS_TYPE_MAC,
    OS_TYPE_FREEBSD,
    OS_TYPE_OTHER
  }
}
