
package net.xdob.ratly.metrics.dropwizard3;

import net.xdob.ratly.metrics.Timekeeper;
import com.codahale.metrics.Timer;

/**
 * The default implementation of {@link Timekeeper} by the shaded {@link Timer}.
 */
public class Dm3TimekeeperImpl implements Timekeeper {
  private final Timer timer;

  Dm3TimekeeperImpl(Timer timer) {
    this.timer = timer;
  }

  public Timer getTimer() {
    return timer;
  }

  @Override
  public Context time() {
    final Timer.Context context = timer.time();
    return context::stop;
  }
}
