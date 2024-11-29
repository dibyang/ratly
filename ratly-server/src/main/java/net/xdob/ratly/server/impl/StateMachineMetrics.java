package net.xdob.ratly.server.impl;

import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;
import net.xdob.ratly.metrics.RatlyMetrics;
import net.xdob.ratly.metrics.Timekeeper;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.statemachine.StateMachine;

import java.util.function.LongSupplier;

/**
 * Metrics Registry for the State Machine Updater. One instance per group.
 */
public final class StateMachineMetrics extends RatlyMetrics {

  public static final String RATLY_STATEMACHINE_METRICS = "state_machine";
  public static final String RATLY_STATEMACHINE_METRICS_DESC = "Metrics for State Machine Updater";

  public static final String STATEMACHINE_APPLIED_INDEX_GAUGE = "appliedIndex";
  public static final String STATEMACHINE_APPLY_COMPLETED_GAUGE = "applyCompletedIndex";
  public static final String STATEMACHINE_TAKE_SNAPSHOT_TIMER = "takeSnapshot";

  public static StateMachineMetrics getStateMachineMetrics(
      RaftServerImpl server, RaftLogIndex appliedIndex,
      StateMachine stateMachine) {

    String serverId = server.getMemberId().toString();
    LongSupplier getApplied = appliedIndex::get;
    LongSupplier getApplyCompleted =
        () -> (stateMachine.getLastAppliedTermIndex() == null) ? -1
            : stateMachine.getLastAppliedTermIndex().getIndex();

    return new StateMachineMetrics(serverId, getApplied, getApplyCompleted);
  }

  private final Timekeeper takeSnapshotTimer = getRegistry().timer(STATEMACHINE_TAKE_SNAPSHOT_TIMER);

  private StateMachineMetrics(String serverId, LongSupplier getApplied,
      LongSupplier getApplyCompleted) {
    super(createRegistry(serverId));

    getRegistry().gauge(STATEMACHINE_APPLIED_INDEX_GAUGE, () -> getApplied::getAsLong);
    getRegistry().gauge(STATEMACHINE_APPLY_COMPLETED_GAUGE, () -> getApplyCompleted::getAsLong);
  }

  private static RatlyMetricRegistry createRegistry(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATLY_APPLICATION_NAME_METRICS,
        RATLY_STATEMACHINE_METRICS, RATLY_STATEMACHINE_METRICS_DESC));
  }

  public Timekeeper getTakeSnapshotTimer() {
    return takeSnapshotTimer;
  }

}