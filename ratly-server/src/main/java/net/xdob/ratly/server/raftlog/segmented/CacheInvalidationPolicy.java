package net.xdob.ratly.server.raftlog.segmented;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.xdob.ratly.server.raftlog.segmented.SegmentedRaftLogCache.LogSegmentList;
import net.xdob.ratly.util.AutoCloseableLock;

public interface CacheInvalidationPolicy {
  /**
   * 确定哪些日志 Segment 应逐出其日志条目缓存
   * @param followerNextIndices the next indices of all the follower peers. Null
   *                            if the local peer is not a leader.
   * @param safeEvictIndex the index up to which cache can be evicted. This
   *                       index depends on two factors:
   *                       1. the largest index belonging to a closed segment
   *                       2. the latest snapshotIndex
   *                       Logs with endIndex less than the max of these two
   *                       indices can be evicted.
   * @param lastAppliedIndex the last index that has been applied to state machine
   * @param segments The list of log segments. The segments should be sorted in
   *                 ascending order according to log index.
   * @param maxCachedSegments the max number of segments with cached log entries
   * @return the log segments that should evict cache
   */
  List<LogSegment> evict(long[] followerNextIndices, long safeEvictIndex,
      long lastAppliedIndex, LogSegmentList segments, int maxCachedSegments);

  class CacheInvalidationPolicyDefault implements CacheInvalidationPolicy {
    @Override
    public List<LogSegment> evict(long[] followerNextIndices,
        long safeEvictIndex, long lastAppliedIndex,
        LogSegmentList segments, final int maxCachedSegments) {
      try(AutoCloseableLock readLock = segments.readLock()) {
        return evictImpl(followerNextIndices, safeEvictIndex, lastAppliedIndex, segments);
      }
    }

    private List<LogSegment> evictImpl(long[] followerNextIndices,
        long safeEvictIndex, long lastAppliedIndex,
        LogSegmentList segments) {
      List<LogSegment> result = new ArrayList<>();
      int safeIndex = segments.size() - 1;
      for (; safeIndex >= 0; safeIndex--) {
        LogSegment segment = segments.get(safeIndex);
        // 只有当段已关闭且所有条目已刷新到本地磁盘，并且本地磁盘段也已关闭时，段的缓存才能被作废。
        if (!segment.isOpen() && segment.getEndIndex() <= safeEvictIndex) {
          break;
        }
      }
      if (followerNextIndices == null || followerNextIndices.length == 0) {
        // 没有跟随者，根据 lastAppliedIndex 确定驱逐。
        // 首先从最旧的段扫描到紧挨着 lastAppliedIndex 的段。所有这些段的缓存都可以被作废。
        int j = 0;
        for (; j <= safeIndex; j++) {
          LogSegment segment = segments.get(j);
          if (segment.getEndIndex() > lastAppliedIndex) {
            break;
          }
          if (segment.hasCache()) {
            result.add(segment);
          }
        }
        // if there is no cache invalidation target found, pick a segment that
        // later (but not now) the state machine will consume
        if (result.isEmpty()) {
          for (int i = safeIndex; i >= j; i--) {
            LogSegment s = segments.get(i);
            if (s.getStartIndex() > lastAppliedIndex && s.hasCache()) {
              result.add(s);
              break;
            }
          }
        }
      } else {
        // this peer is the leader with followers. determine the eviction based
        // on followers' next indices and the local lastAppliedIndex.
        Arrays.sort(followerNextIndices);
        // segments covering index minToRead will still be loaded. Thus we first
        // try to evict cache for segments before minToRead.
        final long minToRead = Math.min(followerNextIndices[0], lastAppliedIndex);
        int j = 0;
        for (; j <= safeIndex; j++) {
          LogSegment s = segments.get(j);
          if (s.getEndIndex() >= minToRead) {
            break;
          }
          if (s.hasCache()) {
            result.add(s);
          }
        }
        // if there is no eviction target, continue the scanning and evict
        // the one that is not being read currently.
        if (result.isEmpty()) {
          for (; j <= safeIndex; j++) {
            LogSegment s = segments.get(j);
            if (Arrays.stream(followerNextIndices).noneMatch(s::containsIndex)
                && !s.containsIndex(lastAppliedIndex) && s.hasCache()) {
              result.add(s);
              break;
            }
          }
        }
      }
      return result;
    }
  }
}
