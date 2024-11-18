

package net.xdob.ratly.server.metrics;

import net.xdob.ratly.metrics.LongCounter;
import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;
import net.xdob.ratly.metrics.RatlyMetrics;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.server.raftlog.LogEntryHeader;

public class RaftLogMetricsBase extends RatlyMetrics implements RaftLogMetrics {
  public static final String RATLY_LOG_WORKER_METRICS_DESC = "Metrics for Log Worker";
  public static final String RATLY_LOG_WORKER_METRICS = "log_worker";

  // Log Entry metrics
  public static final String METADATA_LOG_ENTRY_COUNT = "metadataLogEntryCount";
  public static final String CONFIG_LOG_ENTRY_COUNT = "configLogEntryCount";
  public static final String STATE_MACHINE_LOG_ENTRY_COUNT = "stateMachineLogEntryCount";

  private final LongCounter configLogEntryCount = getRegistry().counter(CONFIG_LOG_ENTRY_COUNT);
  private final LongCounter metadataLogEntryCount = getRegistry().counter(METADATA_LOG_ENTRY_COUNT);
  private final LongCounter stateMachineLogEntryCount = getRegistry().counter(STATE_MACHINE_LOG_ENTRY_COUNT);

  public RaftLogMetricsBase(RaftGroupMemberId serverId) {
    super(createRegistry(serverId));
  }

  public static RatlyMetricRegistry createRegistry(RaftGroupMemberId serverId) {
    return create(new MetricRegistryInfo(serverId.toString(),
        RATLY_APPLICATION_NAME_METRICS,
        RATLY_LOG_WORKER_METRICS, RATLY_LOG_WORKER_METRICS_DESC));
  }

  @Override
  public void onLogEntryCommitted(LogEntryHeader header) {
    switch (header.getLogEntryBodyCase()) {
      case CONFIGURATIONENTRY:
        configLogEntryCount.inc();
        return;
      case METADATAENTRY:
        metadataLogEntryCount.inc();
        return;
      case STATEMACHINELOGENTRY:
        stateMachineLogEntryCount.inc();
        return;
      default:
    }
  }
}
