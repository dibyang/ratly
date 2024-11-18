
package net.xdob.ratly.shell.cli.sh.command;

import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.client.RaftClientConfigKeys;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.retry.ExponentialBackoffRetry;
import net.xdob.ratly.retry.RetryPolicy;
import com.google.common.io.Closer;
import net.xdob.ratly.util.TimeDuration;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A context for ratly-shell.
 */
public final class Context implements Closeable {
  private static final TimeDuration DEFAULT_REQUEST_TIMEOUT = TimeDuration.valueOf(15, TimeUnit.SECONDS);
  private static final RetryPolicy DEFAULT_RETRY_POLICY = ExponentialBackoffRetry.newBuilder()
      .setBaseSleepTime(TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS))
      .setMaxAttempts(10)
      .setMaxSleepTime(TimeDuration.valueOf(100_000, TimeUnit.MILLISECONDS))
      .build();

  private final PrintStream mPrintStream;
  private final Closer mCloser;

  private final boolean cli;
  private final RetryPolicy retryPolicy;
  private final RaftProperties properties;
  private final Parameters parameters;

  /**
   * Build a context.
   * @param printStream the print stream
   */
  public Context(PrintStream printStream) {
    this(printStream, true, DEFAULT_RETRY_POLICY, new RaftProperties(), null);
  }

  public Context(PrintStream printStream, boolean cli, RetryPolicy retryPolicy,
      RaftProperties properties, Parameters parameters) {
    mCloser = Closer.create();
    mPrintStream = mCloser.register(Objects.requireNonNull(printStream, "printStream == null"));

    this.cli = cli;
    this.retryPolicy = retryPolicy != null? retryPolicy : DEFAULT_RETRY_POLICY;
    this.properties = properties != null? properties : new RaftProperties();
    this.parameters = parameters;
  }

  /**
   * @return the print stream to write to
   */
  public PrintStream getPrintStream() {
    return mPrintStream;
  }

  /** Is this from CLI? */
  public boolean isCli() {
    return cli;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public RaftProperties getProperties() {
    return properties;
  }

  public Parameters getParameters() {
    return parameters;
  }

  /** Create a new {@link RaftClient} from the given group. */
  public RaftClient newRaftClient(RaftGroup group) {
    final RaftProperties p = getProperties();
    if (isCli()) {
      RaftClientConfigKeys.Rpc.setRequestTimeout(p, DEFAULT_REQUEST_TIMEOUT);

      // Since ratly-shell support GENERIC_COMMAND_OPTIONS, here we should
      // merge these options to raft p to make it work.
      final Properties sys = System.getProperties();
      sys.stringPropertyNames().forEach(key -> p.set(key, sys.getProperty(key)));
    }

    return RaftClient.newBuilder()
        .setRaftGroup(group)
        .setProperties(p)
        .setParameters(getParameters())
        .setRetryPolicy(getRetryPolicy())
        .build();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
