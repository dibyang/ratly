
package net.xdob.ratly.server.raftlog;

import net.xdob.ratly.proto.RaftProtos.LogEntryProto;
import net.xdob.ratly.proto.RaftProtos.LogEntryProto.LogEntryBodyCase;
import net.xdob.ratly.server.protocol.TermIndex;

import java.util.Comparator;

/** The header of a {@link LogEntryProto} including {@link TermIndex} and {@link LogEntryBodyCase}. */
public interface LogEntryHeader extends Comparable<LogEntryHeader> {
  LogEntryHeader[] EMPTY_ARRAY = {};

  /** @return the {@link TermIndex}. */
  TermIndex getTermIndex();

  default long getTerm() {
    return getTermIndex().getTerm();
  }

  default long getIndex() {
    return getTermIndex().getIndex();
  }

  /** @return the {@link LogEntryBodyCase}. */
  LogEntryBodyCase getLogEntryBodyCase();

  static LogEntryHeader valueOf(LogEntryProto entry) {
    return valueOf(TermIndex.valueOf(entry), entry.getLogEntryBodyCase());
  }

  static LogEntryHeader valueOf(TermIndex ti, LogEntryBodyCase logEntryBodyCase) {
    return new LogEntryHeader() {
      @Override
      public TermIndex getTermIndex() {
        return ti;
      }

      @Override
      public LogEntryBodyCase getLogEntryBodyCase() {
        return logEntryBodyCase;
      }

      @Override
      public int compareTo(LogEntryHeader that) {
        return Comparator.comparing(LogEntryHeader::getTermIndex)
            .thenComparing(LogEntryHeader::getLogEntryBodyCase)
            .compare(this, that);
      }

      @Override
      public int hashCode() {
        return ti.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        } else if (!(obj instanceof LogEntryHeader)) {
          return false;
        }

        final LogEntryHeader that = (LogEntryHeader) obj;
        return this.getLogEntryBodyCase() == that.getLogEntryBodyCase()
            && this.getTermIndex().equals(that.getTermIndex());
      }
    };
  }
}
