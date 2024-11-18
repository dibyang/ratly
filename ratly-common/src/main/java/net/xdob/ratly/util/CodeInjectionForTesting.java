package net.xdob.ratly.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Inject code for testing. */
public final class CodeInjectionForTesting {

  private CodeInjectionForTesting() {
  }

  public static final Logger LOG = LoggerFactory.getLogger(CodeInjectionForTesting.class);

  /** Code to be injected. */
  public interface Code {
    Logger LOG = CodeInjectionForTesting.LOG;

    /**
     * Execute the injected code for testing.
     * @param localId the id of the local peer
     * @param remoteId the id of the remote peer if handling a request
     * @param args other possible args
     * @return if the injected code is executed
     */
    boolean execute(Object localId, Object remoteId, Object... args);
  }

  private static final Map<String, Code> INJECTION_POINTS
      = new ConcurrentHashMap<>();

  /** Put an injection point. */
  public static void put(String injectionPoint, Code code) {
    LOG.debug("put: {}, {}", injectionPoint, code);
    INJECTION_POINTS.put(injectionPoint, code);
  }

  /** Execute the injected code, if there is any. */
  public static boolean execute(String injectionPoint, Object localId,
      Object remoteId, Object... args) {
    final Code code = INJECTION_POINTS.get(injectionPoint);
    if (code == null) {
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute: {}, {}, localId={}, remoteId={}, args={}",
          injectionPoint, code, localId, remoteId, Arrays.toString(args));
    }
    return code.execute(localId, remoteId, args);
  }

  /** Remove an injection point. */
  public static void remove(String injectionPoint) {
    INJECTION_POINTS.remove(injectionPoint);
  }
}
