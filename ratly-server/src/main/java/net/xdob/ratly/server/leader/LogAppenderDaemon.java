package net.xdob.ratly.server.leader;

import net.xdob.ratly.util.Daemon;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.LifeCycle;

import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import net.xdob.ratly.util.LifeCycle.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.xdob.ratly.util.LifeCycle.State.CLOSED;
import static net.xdob.ratly.util.LifeCycle.State.CLOSING;
import static net.xdob.ratly.util.LifeCycle.State.EXCEPTION;
import static net.xdob.ratly.util.LifeCycle.State.NEW;
import static net.xdob.ratly.util.LifeCycle.State.RUNNING;
import static net.xdob.ratly.util.LifeCycle.State.STARTING;

class LogAppenderDaemon {
  public static final Logger LOG = LoggerFactory.getLogger(LogAppenderDaemon.class);

  private final String name;
  private final LifeCycle lifeCycle;
  private final Daemon daemon;

  private final LogAppenderBase logAppender;
  private final CompletableFuture<State> closeFuture = new CompletableFuture<>();

  LogAppenderDaemon(LogAppenderBase logAppender) {
    this.logAppender = logAppender;
    this.name = logAppender + "-" + JavaUtils.getClassSimpleName(getClass());
    this.lifeCycle = new LifeCycle(name);
    this.daemon = Daemon.newBuilder().setName(name).setRunnable(this::run)
        .setThreadGroup(logAppender.getServer().getThreadGroup()).build();
  }

  public boolean isWorking() {
    return !LifeCycle.States.CLOSING_OR_CLOSED_OR_EXCEPTION.contains(lifeCycle.getCurrentState());
  }

  public void tryToStart() {
    if (lifeCycle.compareAndTransition(NEW, STARTING)) {
      daemon.start();
    }
  }

  static final UnaryOperator<State> TRY_TO_RUN = current -> {
    if (current == STARTING) {
      return RUNNING;
    } else if (LifeCycle.States.CLOSING_OR_CLOSED_OR_EXCEPTION.contains(current)) {
      return current;
    }
    // Other states are illegal.
    throw new IllegalArgumentException("Cannot to tryToRun from " + current);
  };

  private void run() {
    try {
      if (lifeCycle.transition(TRY_TO_RUN) == RUNNING) {
        logAppender.run();
      }
      lifeCycle.compareAndTransition(RUNNING, CLOSING);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info(this + " was interrupted: " + e);
    } catch (InterruptedIOException e) {
      LOG.info(this + " I/O was interrupted: " + e);
    } catch (Throwable e) {
      LOG.warn(this + " failed", e);
      lifeCycle.transitionIfValid(EXCEPTION);
    } finally {
      final State finalState = lifeCycle.transitionAndGet(TRANSITION_FINALLY);
      if (finalState == EXCEPTION) {
        logAppender.restart();
      }
      closeFuture.complete(finalState);
    }
  }

  static final UnaryOperator<State> TRANSITION_FINALLY = current -> {
    if (State.isValid(current, CLOSED)) {
      return CLOSED;
    } else if (State.isValid(current, EXCEPTION)) {
      return EXCEPTION;
    } else {
      return current;
    }
  };

  public CompletableFuture<State> tryToClose() {
    final State state = lifeCycle.transition(TRY_TO_CLOSE);
    if (state == CLOSING) {
      daemon.interrupt();
    } else if (state == CLOSED) {
      closeFuture.complete(state);
    }
    return closeFuture;
  }

  static final UnaryOperator<State> TRY_TO_CLOSE = current -> {
    if (current == NEW) {
      return CLOSED;
    } else if (current.isClosingOrClosed()) {
      return current;
    } else if (State.isValid(current, CLOSING)) {
      return CLOSING;
    }
    // Other states are illegal.
    throw new IllegalArgumentException("Cannot to tryToClose from " + current);
  };

  @Override
  public String toString() {
    return name;
  }
}
