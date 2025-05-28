package net.xdob.ratly.util;

import net.xdob.ratly.util.function.CheckedRunnable;
import net.xdob.ratly.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

/**
 * 使用有限状态机（FSM）设计，每个状态有明确的前置状态和定义，保证状态流转的逻辑性和可维护性。
 * 状态机的生命周期。
 * <pre>
 *   -------------------------------------------------
 *  |        --------------------------------         |
 *  |       |     ------------------------   |        |
 *  |       |    |                        |  |        |
 *  |       |  PAUSED <---- PAUSING----   |  |        |
 *  |       |    |          ^     |    |  |  |        |
 *  |       |    V          |     |    V  V  V        V
 * NEW --> STARTING --> RUNNING --|--> CLOSING --> [CLOSED]
 *  ^       |    |          |     |       ^
 *  |       |    |          V     V       |
 *   -------      -------> EXCEPTION -----
 * </pre>
 */
public class LifeCycle {
  public static final Logger LOG = LoggerFactory.getLogger(LifeCycle.class);

  /** 生命周期中的状态。 */
  public enum State {
    /** 该状态机是新创建的，占用的资源为零。 */
    NEW,
    /** 状态机正在启动，但尚未提供任何服务。 */
    STARTING,
    /** 状态机正在运行并提供服务。 */
    RUNNING,
    /** 状态机正在暂停和停止提供服务。 */
    PAUSING,
    /** 状态机已暂停，不提供任何服务。 */
    PAUSED,
    /** 状态机捕获内部异常，因此必须将其关闭。 */
    EXCEPTION,
    /** 状态机正在关闭、正在停止提供服务和释放资源。*/
    CLOSING,
    /** 状态机关闭，这是最终状态。 */
    CLOSED;

    private static final Map<State, List<State>> PREDECESSORS;

    /** Is this {@link State#RUNNING}? */
    public boolean isRunning() {
      return this == RUNNING;
    }

    /** Is this {@link State#CLOSING} or {@link State#CLOSED}? */
    public boolean isClosingOrClosed() {
      return States.CLOSING_OR_CLOSED.contains(this);
    }

    /** Is this {@link State#PAUSING} or {@link State#PAUSED}? */
    public boolean isPausingOrPaused() {
      return States.PAUSING_OR_PAUSED.contains(this);
    }

    /**
     *
     * @param key 目标状态
     * @param map 状态关系存储
     * @param values 可以到达目标状态的原始状态
     */
    static void put(State key, Map<State, List<State>> map, State... values) {
      map.put(key, Collections.unmodifiableList(Arrays.asList(values)));
    }

    static {
      final Map<State, List<State>> predecessors = new EnumMap<>(State.class);

      put(NEW,       predecessors, STARTING);
      put(STARTING,  predecessors, NEW, PAUSED);
      put(RUNNING,   predecessors, STARTING);
      put(PAUSING,   predecessors, RUNNING);
      put(PAUSED,    predecessors, PAUSING);
      put(EXCEPTION, predecessors, STARTING, PAUSING, RUNNING);
      put(CLOSING,   predecessors, STARTING, RUNNING, PAUSING, PAUSED, EXCEPTION);
      put(CLOSED,    predecessors, NEW, CLOSING);

      PREDECESSORS = Collections.unmodifiableMap(predecessors);
    }

    /** Is the given transition valid? */
    public static boolean isValid(State from, State to) {
      return PREDECESSORS.get(to).contains(from);
    }

    /** Validate the given transition. */
    static void validate(Object name, State from, State to) {
      LOG.debug("{}: {} -> {}", name, from, to);
      if (LOG.isTraceEnabled()) {
        LOG.trace("TRACE", new Throwable());
      }

      Preconditions.assertTrue(isValid(from, to),
          "ILLEGAL TRANSITION: In %s, %s -> %s", name, from, to);
    }
  }

  public static final class States {

    public static final Set<State> RUNNING
        = Collections.unmodifiableSet(EnumSet.of(State.RUNNING));

    public static final Set<State> STARTING_OR_RUNNING
        = Collections.unmodifiableSet(EnumSet.of(State.STARTING, State.RUNNING));

    public static final Set<State> CLOSING_OR_CLOSED
        = Collections.unmodifiableSet(EnumSet.of(State.CLOSING, State.CLOSED));

    public static final Set<State> PAUSING_OR_PAUSED
        = Collections.unmodifiableSet(EnumSet.of(State.PAUSING, State.PAUSED));

    public static final Set<State> CLOSING_OR_CLOSED_OR_EXCEPTION
        = Collections.unmodifiableSet(EnumSet.of(State.CLOSING, State.CLOSED, State.EXCEPTION));

    private States() {
      // no instances
    }

  }

  private volatile String name;
  private final AtomicReference<State> current = new AtomicReference<>(State.NEW);

  public LifeCycle(Object name) {
    this.name = name.toString();
    LOG.debug("{}: {}", name, current);
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * 从当前状态无条件转换到目标状态。
   */
  public void transition(final State to) {
    current.updateAndGet(from -> {
      State.validate(name, from, to);
      return to;
    });
  }

  /**
   * 如果当前状态不等于给定状态，则从当前状态转换到给定状态。
   */
  public void transitionIfNotEqual(final State to) {
    current.updateAndGet(from -> {
      if (from != to) {
        State.validate(name, from, to);
      }
      return to;
    });
  }

  /**
   * 仅当转换有效时，才从当前状态转换到给定状态。
   * 如果转换无效，则为 no-op。
   *
   * @return 如果更新的状态等于给定的状态，则为 true。
   */
  public boolean transitionIfValid(final State to) {
    final State updated = current.updateAndGet(from -> State.isValid(from, to)? to : from);
    return updated == to;
  }

  /**
   * 通过操作符转换状态.
   *
   * @return 如果有转换，则为 转换后的状态; 否则，返回 null 以指示无状态更改。
   */
  public State transition(UnaryOperator<State> operator) {
    for(;;) {
      final State previous = current.get();
      final State applied = operator.apply(previous);
      if (previous == applied) {
        return null; // no change required
      }
      State.validate(name, previous, applied);
      if (current.compareAndSet(previous, applied)) {
        return applied;
      }
      // state has been changed, retry
    }
  }

  /**
   * 通过操作符转换状态，返回最终状态。
   *
   * @return 转换后的状态。
   */
  public State transitionAndGet(UnaryOperator<State> operator) {
    return current.updateAndGet(previous -> {
      final State applied = operator.apply(previous);
      if (applied != previous) {
        State.validate(name, previous, applied);
      }
      return applied;
    });
  }

  /**
   * 如果当前状态等于指定的 from 状态，则转换到 give to 状态;否则，不进行任何转换。
   *
   * @return 如果当前状态等于指定的 from 状态返回true，表示转换成功。
   */
  public boolean compareAndTransition(final State from, final State to) {
    final State previous = current.getAndUpdate(state -> {
      if (state != from) {
        return state;
      }
      State.validate(name, from, to);
      return to;
    });
    return previous == from;
  }

  /** @return 当前状态。 */
  public State getCurrentState() {
    return current.get();
  }

  /** 如果当前状态等于预期状态之一，则断言。 */
  public void assertCurrentState(Set<State> expected) {
    assertCurrentState((n, c) -> new IllegalStateException("STATE MISMATCHED: In "
        + n + ", current state " + c + " is not one of the expected states "
        + expected), expected);
  }

  /** 如果当前状态等于预期状态之一，则断言。 */
  public <T extends Throwable> State assertCurrentState(
      BiFunction<String, State, T> newThrowable, Set<State> expected) throws T {
    final State c = getCurrentState();
    if (!expected.contains(c)) {
      throw newThrowable.apply(name, c);
    }
    return c;
  }

  @Override
  public String toString() {
    return name + ":" + getCurrentState();
  }

  /**
   * 运行给定的 start 方法并相应地转换当前状态。
   */
  @SafeVarargs
  public final <T extends Throwable> void startAndTransition(
      CheckedRunnable<T> startImpl, Class<? extends Throwable>... exceptionClasses)
      throws T {
    transition(State.STARTING);
    try {
      startImpl.run();
      transition(State.RUNNING);
    } catch (Throwable t) {
      transition(ReflectionUtils.isInstance(t, exceptionClasses)?
          State.NEW: State.EXCEPTION);
      throw t;
    }
  }

  /**
   * 检查当前状态，并在适用的情况下转换为 {@link State#CLOSING}。
   * 如果当前状态已经是 {@link State#CLOSING} 或 {@link State#CLOSED}，
   * 则调用此方法不会产生任何效果。
   * 换句话说，此方法可以安全地多次调用。
   */
  public State checkStateAndClose() {
    return checkStateAndClose(() -> State.CLOSING);
  }

  /**
   * 检查当前状态，并在适用的情况下执行指定的关闭方法。
   * 如果当前状态已经是 {@link State#CLOSING} 或 {@link State#CLOSED}，
   * 则调用此方法不会产生任何效果。
   * 换句话说，此方法可以安全地多次调用，
   * 而指定的关闭方法最多只会被执行一次。
   */
  public <T extends Throwable> State checkStateAndClose(CheckedRunnable<T> closeMethod) throws T {
    return checkStateAndClose(() -> {
      try {
        closeMethod.run();
      } finally {
        transition(State.CLOSED);
      }
      return State.CLOSED;
    });
  }

  private <T extends Throwable> State checkStateAndClose(CheckedSupplier<State, T> closeMethod) throws T {
    if (compareAndTransition(State.NEW, State.CLOSED)) {
      return State.CLOSED;
    }

    for(;;) {
      final State c = getCurrentState();
      if (c.isClosingOrClosed()) {
        return c; //already closing or closed.
      }

      if (compareAndTransition(c, State.CLOSING)) {
        return closeMethod.get();
      }

      // lifecycle state is changed, retry.
    }
  }
}
