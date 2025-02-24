package net.xdob.ratly.server.raftlog.segmented;

import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.util.Preconditions;

import java.io.File;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 日志 Segment 的开始索引和结束索引。
 * <p>
 * 用于描述日志段文件的包含日志的起始和终止索引。
 */
public final class LogSegmentStartEnd implements Comparable<LogSegmentStartEnd> {
  private static final String LOG_FILE_NAME_PREFIX = "log";
  private static final String IN_PROGRESS = "inprogress";
  private static final Pattern CLOSED_SEGMENT_PATTERN;
  private static final Pattern OPEN_SEGMENT_PATTERN;

  static {
    final String digits = "(\\d+)";
    CLOSED_SEGMENT_PATTERN = Pattern.compile(LOG_FILE_NAME_PREFIX + "_" + digits + "-" + digits);
    OPEN_SEGMENT_PATTERN = Pattern.compile(LOG_FILE_NAME_PREFIX + "_" + IN_PROGRESS + "_" + digits + "(?:\\..*)?");
  }

  private static String getOpenLogFileName(long startIndex) {
    return LOG_FILE_NAME_PREFIX + "_" + IN_PROGRESS + "_" + startIndex;
  }

  static Pattern getOpenSegmentPattern() {
    return OPEN_SEGMENT_PATTERN;
  }

  private static String getClosedLogFileName(long startIndex, long endIndex) {
    return LOG_FILE_NAME_PREFIX + "_" + startIndex + "-" + endIndex;
  }

  static Pattern getClosedSegmentPattern() {
    return CLOSED_SEGMENT_PATTERN;
  }

  static LogSegmentStartEnd valueOf(long startIndex) {
    return new LogSegmentStartEnd(startIndex, null);
  }

  static LogSegmentStartEnd valueOf(long startIndex, Long endIndex) {
    return new LogSegmentStartEnd(startIndex, endIndex);
  }

  static LogSegmentStartEnd valueOf(long startIndex, long endIndex, boolean isOpen) {
    return new LogSegmentStartEnd(startIndex, isOpen? null: endIndex);
  }
  // startIndex 起始索引不能为空
  private final long startIndex;
  // endIndex 结束索引可以为空，当日志段是最新使用中的段时结束索引为空
  private final Long endIndex;

  private LogSegmentStartEnd(long startIndex, Long endIndex) {
    this.startIndex = startIndex;
    this.endIndex = endIndex;

    Preconditions.assertTrue(startIndex >= RaftLog.LEAST_VALID_LOG_INDEX, this);
    if (endIndex != null) {
      Preconditions.assertTrue(endIndex >= startIndex, this);
    }
  }

  long getStartIndex() {
    return startIndex;
  }

  long getEndIndex() {
    return Objects.requireNonNull(endIndex, "endIndex");
  }

  boolean isOpen() {
    return endIndex == null;
  }

  private String getFileName() {
    return isOpen()? getOpenLogFileName(startIndex): getClosedLogFileName(startIndex, endIndex);
  }

  File getFile(File dir) {
    return new File(dir, getFileName());
  }

  File getFile(RaftStorage storage) {
    return getFile(storage.getStorageDir().getCurrentDir());
  }

  @Override
  public int compareTo(LogSegmentStartEnd that) {
    if (this == that) {
      return 0;
    }
    // startIndex always non-null
    final int diff = Long.compare(this.getStartIndex(), that.getStartIndex());
    if (diff != 0) {
      return diff;
    }

    // same startIndex, compare endIndex
    if (this.isOpen()) {
      return that.isOpen()? 0 : -1; //open first
    } else {
      return that.isOpen() ? 1 : Long.compare(this.endIndex, that.endIndex);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final LogSegmentStartEnd that = (LogSegmentStartEnd) obj;
    return startIndex == that.startIndex && Objects.equals(endIndex, that.endIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startIndex, endIndex);
  }

  @Override
  public String toString() {
    return startIndex + "-" + (endIndex != null? endIndex : "");
  }
}