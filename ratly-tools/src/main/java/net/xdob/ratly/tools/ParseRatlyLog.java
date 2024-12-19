

package net.xdob.ratly.tools;

import net.xdob.ratly.proto.raft.StateMachineLogEntryProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.config.CorruptionPolicy;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.server.raftlog.segmented.LogSegmentPath;
import net.xdob.ratly.server.raftlog.segmented.LogSegment;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

public final class ParseRatlyLog {

  private final File file;
  private final Function<StateMachineLogEntryProto, String> smLogToString;
  private final SizeInBytes maxOpSize;

  private long numConfEntries;
  private long numMetadataEntries;
  private long numStateMachineEntries;
  private long numInvalidEntries;

  private ParseRatlyLog(File f , Function<StateMachineLogEntryProto, String> smLogToString, SizeInBytes maxOpSize) {
    this.file = f;
    this.smLogToString = smLogToString;
    this.maxOpSize = maxOpSize;
    this.numConfEntries = 0;
    this.numMetadataEntries = 0;
    this.numStateMachineEntries = 0;
    this.numInvalidEntries = 0;
  }

  public void dumpSegmentFile() throws IOException {
    final LogSegmentPath pi = LogSegmentPath.matchLogSegment(file.toPath());
    if (pi == null) {
      System.out.println("Invalid segment file");
      return;
    }

    System.out.println("Processing Raft Log file: " + file.getAbsolutePath() + " size:" + file.length());
    final int entryCount = LogSegment.readSegmentFile(file, pi.getStartEnd(), maxOpSize,
        CorruptionPolicy.EXCEPTION, null, this::processLogEntry);
    System.out.println("Num Total Entries: " + entryCount);
    System.out.println("Num Conf Entries: " + numConfEntries);
    System.out.println("Num Metadata Entries: " + numMetadataEntries);
    System.out.println("Num StateMachineEntries Entries: " + numStateMachineEntries);
    System.out.println("Num Invalid Entries: " + numInvalidEntries);
  }


  private void processLogEntry(ReferenceCountedObject<LogEntryProto> ref) {
    final LogEntryProto proto = ref.retain();
    if (proto.hasConfigurationEntry()) {
      numConfEntries++;
    } else if (proto.hasMetadataEntry()) {
      numMetadataEntries++;
    } else if (proto.hasStateMachineLogEntry()) {
      numStateMachineEntries++;
    } else {
      System.out.println("Found an invalid entry: " + proto);
      numInvalidEntries++;
    }

    String str = LogProtoUtils.toLogEntryString(proto, smLogToString);
    System.out.println(str);
    ref.release();
  }

  public static class Builder {
    private File file = null;
    private Function<StateMachineLogEntryProto, String> smLogToString = null;
    private SizeInBytes maxOpSize = SizeInBytes.valueOf("32MB");

    public Builder setMaxOpSize(SizeInBytes maxOpSize) {
      this.maxOpSize = maxOpSize;
      return this;
    }

    public Builder setSegmentFile(File segmentFile) {
      this.file = segmentFile;
      return this;
    }

    public Builder setSMLogToString(Function<StateMachineLogEntryProto, String> smLogToStr) {
      this.smLogToString = smLogToStr;
      return this;
    }

    public ParseRatlyLog build() {
      return new ParseRatlyLog(file, smLogToString, maxOpSize);
    }
  }
}
