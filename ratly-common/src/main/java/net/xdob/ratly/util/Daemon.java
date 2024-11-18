
package net.xdob.ratly.util;

import java.util.Objects;

public class Daemon extends Thread {
  {
    setDaemon(true);
  }

  /** Construct a daemon thread with flexible arguments. */
  protected Daemon(Builder builder) {
    super(builder.threadGroup, builder.runnable);
    setName(builder.name);
  }

  /** @return a {@link Builder}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private Runnable runnable;
    private ThreadGroup threadGroup;

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setRunnable(Runnable runnable) {
      this.runnable = runnable;
      return this;
    }

    public Builder setThreadGroup(ThreadGroup threadGroup) {
      this.threadGroup = threadGroup;
      return this;
    }

    public Daemon build() {
      Objects.requireNonNull(name, "name == null");
      return new Daemon(this);
    }
  }
}
