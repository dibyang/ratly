
package net.xdob.ratly.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Facilitates hooking process termination for tests and debugging. */
public interface ExitUtils {
  class ExitException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final int status;

    ExitException(int status, String message, Throwable throwable) {
      super(message, throwable);
      this.status = status;
    }

    public int getStatus() {
      return status;
    }
  }

  final class States {
    private static final Logger LOG = LoggerFactory.getLogger(ExitUtils.class);
    private static final States INSTANCE = new States();

    private volatile boolean systemExitDisabled = false;
    private volatile boolean terminateOnUncaughtException = true;
    private final AtomicReference<ExitException> firstExitException = new AtomicReference<>();

    private States() {
      Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
        if (terminateOnUncaughtException) {
          terminate(-1, thread + " has thrown an uncaught exception", exception, false, LOG);
        }
      });
    }

    private void setTerminateOnUncaughtException(boolean terminateOnUncaughtException) {
      this.terminateOnUncaughtException = terminateOnUncaughtException;
    }

    private void disableSystemExit() {
      systemExitDisabled = true;
    }

    private boolean isSystemExitDisabled() {
      return systemExitDisabled;
    }

    private ExitException getFirstExitException() {
      return firstExitException.get();
    }

    private boolean setFirstExitException(ExitException e) {
      Objects.requireNonNull(e, "e == null");
      return firstExitException.compareAndSet(null, e);
    }

    private boolean clearFirstExitException() {
      return firstExitException.getAndSet(null) != null;
    }
  }

  /**
   * @return the first {@link ExitException} thrown, or null if none thrown yet.
   */
  static ExitException getFirstExitException() {
    return States.INSTANCE.getFirstExitException();
  }

  /**
   * Clear all previous terminate(..) calls, if there are any.
   *
   * @return true if the state is changed.
   */
  static boolean clear() {
    return States.INSTANCE.clearFirstExitException();
  }

  /** @return true if one of the terminate(..) methods has been invoked. */
  static boolean isTerminated() {
    return getFirstExitException() != null;
  }

  /** @throws AssertionError if {@link #isTerminated()} == true. */
  static void assertNotTerminated() {
    if (ExitUtils.isTerminated()) {
      throw new AssertionError("Unexpected exit.", getFirstExitException());
    }
  }

  /** Disable the use of {@link System#exit(int)}. */
  static void disableSystemExit() {
    States.INSTANCE.disableSystemExit();
  }

  /**
   *
   *
   * @param status Exit status
   * @param message message used to create the {@code ExitException}
   * @param throwExitException decide if this method should throw {@link ExitException}
   * @throws ExitException if throwExitException == true and System.exit is disabled.
   */
  static void terminate(int status, String message, Throwable throwable, boolean throwExitException, Logger log)
      throws ExitException {
    if (log != null) {
      final String s = "Terminating with exit status " + status + ": " + message;
      if (status == 0) {
        log.info(s, throwable);
      } else {
        log.error(s, throwable);
      }
    }

    if (!States.INSTANCE.isSystemExitDisabled()) {
      System.exit(status);
    }

    final ExitException ee = new ExitException(status, message, throwable);

    States.INSTANCE.setFirstExitException(ee);

    if (throwExitException) {
      throw ee;
    }
  }

  static void terminate(int status, String message, Throwable throwable, Logger log) {
    terminate(status, message, throwable, true, log);
  }

  static void terminate(int status, String message, Logger log) {
    terminate(status, message, null, log);
  }

  static void setTerminateOnUncaughtException(boolean terminateOnUncaughtException) {
    States.INSTANCE.setTerminateOnUncaughtException(terminateOnUncaughtException);
  }
}
