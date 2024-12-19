
package net.xdob.ratly.server.raftlog.segmented;

import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.config.CorruptionPolicy;
import net.xdob.ratly.server.metrics.SegmentedRaftLogMetrics;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.LogEntryHeader;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.server.storage.RaftStorage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import com.google.protobuf.CodedOutputStream;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


/**
 * 日志段文件的内存缓存。
 * 将首先写入所有更新放入 LogSegment 中，然后以相同的顺序放入相应的文件中。
 * <p>
 * 此类将受到 {@link SegmentedRaftLog} 的读写锁的保护。
 */
public final class LogSegment {

  static final Logger LOG = LoggerFactory.getLogger(LogSegment.class);

  enum Op {
    LOAD_SEGMENT_FILE,
    REMOVE_CACHE,
    CHECK_SEGMENT_FILE_FULL,
    WRITE_CACHE_WITH_STATE_MACHINE_CACHE,
    WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE
  }

  static long getEntrySize(LogEntryProto entry, Op op) {
    switch (op) {
      case CHECK_SEGMENT_FILE_FULL:
      case LOAD_SEGMENT_FILE:
      case WRITE_CACHE_WITH_STATE_MACHINE_CACHE:
        Preconditions.assertTrue(!LogProtoUtils.hasStateMachineData(entry),
            () -> "Unexpected LogEntryProto with StateMachine data: op=" + op + ", entry=" + entry);
        break;
      case WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE:
      case REMOVE_CACHE:
        break;
      default:
        throw new IllegalStateException("Unexpected op " + op + ", entry=" + entry);
    }
    final int serialized = entry.getSerializedSize();
    return serialized + CodedOutputStream.computeUInt32SizeNoTag(serialized) + 4L;
  }

  static class LogRecord {
    /** starting offset in the file */
    private final long offset;
    private final LogEntryHeader logEntryHeader;

    LogRecord(long offset, LogEntryProto entry) {
      this.offset = offset;
      this.logEntryHeader = LogEntryHeader.valueOf(entry);
    }

    LogEntryHeader getLogEntryHeader() {
      return logEntryHeader;
    }

    TermIndex getTermIndex() {
      return getLogEntryHeader().getTermIndex();
    }

    long getOffset() {
      return offset;
    }
  }

  private static class Records {
    private final ConcurrentNavigableMap<Long, LogRecord> map = new ConcurrentSkipListMap<>();

    int size() {
      return map.size();
    }

    LogRecord getFirst() {
      final Map.Entry<Long, LogRecord> first = map.firstEntry();
      return first != null? first.getValue() : null;
    }

    LogRecord getLast() {
      final Map.Entry<Long, LogRecord> last = map.lastEntry();
      return last != null? last.getValue() : null;
    }

    LogRecord get(long i) {
      return map.get(i);
    }

    long append(LogRecord record) {
      final long index = record.getTermIndex().getIndex();
      final LogRecord previous = map.put(index, record);
      Preconditions.assertNull(previous, "previous");
      return index;
    }

    LogRecord removeLast() {
      final Map.Entry<Long, LogRecord> last = map.pollLastEntry();
      return Objects.requireNonNull(last, "last == null").getValue();
    }

    void clear() {
      map.clear();
    }
  }

  static LogSegment newOpenSegment(RaftStorage storage, long start, SizeInBytes maxOpSize,
      SegmentedRaftLogMetrics raftLogMetrics) {
    Preconditions.assertTrue(start >= 0);
    return new LogSegment(storage, true, start, start - 1, maxOpSize, raftLogMetrics);
  }

  @VisibleForTesting
  static LogSegment newCloseSegment(RaftStorage storage,
      long start, long end, SizeInBytes maxOpSize, SegmentedRaftLogMetrics raftLogMetrics) {
    Preconditions.assertTrue(start >= 0 && end >= start);
    return new LogSegment(storage, false, start, end, maxOpSize, raftLogMetrics);
  }

  static LogSegment newLogSegment(RaftStorage storage, LogSegmentStartEnd startEnd, SizeInBytes maxOpSize,
      SegmentedRaftLogMetrics metrics) {
    return startEnd.isOpen()? newOpenSegment(storage, startEnd.getStartIndex(), maxOpSize, metrics)
        : newCloseSegment(storage, startEnd.getStartIndex(), startEnd.getEndIndex(), maxOpSize, metrics);
  }

  public static int readSegmentFile(File file, LogSegmentStartEnd startEnd, SizeInBytes maxOpSize,
      CorruptionPolicy corruptionPolicy, SegmentedRaftLogMetrics raftLogMetrics,
      Consumer<ReferenceCountedObject<LogEntryProto>> entryConsumer)
      throws IOException {
    int count = 0;
    try(SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(file, startEnd, maxOpSize, raftLogMetrics)) {
      for(LogEntryProto prev = null, next; (next = in.nextEntry()) != null; prev = next) {
        if (prev != null) {
          Preconditions.assertTrue(next.getIndex() == prev.getIndex() + 1,
              "gap between entry %s and entry %s", prev, next);
        }

        if (entryConsumer != null) {
          // TODO: use reference count to support zero buffer copying for readSegmentFile
          entryConsumer.accept(ReferenceCountedObject.wrap(next));
        }
        count++;
      }
    } catch (IOException ioe) {
      switch (corruptionPolicy) {
        case EXCEPTION: throw ioe;
        case WARN_AND_RETURN:
          LOG.warn("Failed to read segment file {} ({}): only {} entries read successfully",
              file, startEnd, count, ioe);
          break;
        default:
          throw new IllegalStateException("Unexpected enum value: " + corruptionPolicy
              + ", class=" + CorruptionPolicy.class);
      }
    }

    return count;
  }

  static LogSegment loadSegment(RaftStorage storage, File file, LogSegmentStartEnd startEnd, SizeInBytes maxOpSize,
      boolean keepEntryInCache, Consumer<LogEntryProto> logConsumer, SegmentedRaftLogMetrics raftLogMetrics)
      throws IOException {
    final LogSegment segment = newLogSegment(storage, startEnd, maxOpSize, raftLogMetrics);
    final CorruptionPolicy corruptionPolicy = CorruptionPolicy.get(storage, RaftStorage::getLogCorruptionPolicy);
    final boolean isOpen = startEnd.isOpen();
    final int entryCount = readSegmentFile(file, startEnd, maxOpSize, corruptionPolicy, raftLogMetrics, entry -> {
      segment.append(Op.LOAD_SEGMENT_FILE, entry, keepEntryInCache || isOpen, logConsumer);
    });
    LOG.info("Successfully read {} entries from segment file {}", entryCount, file);

    final long start = startEnd.getStartIndex();
    final long end = isOpen? segment.getEndIndex(): startEnd.getEndIndex();
    final int expectedEntryCount = Math.toIntExact(end - start + 1);
    final boolean corrupted = entryCount != expectedEntryCount;
    if (corrupted) {
      LOG.warn("Segment file is corrupted: expected to have {} entries but only {} entries read successfully",
          expectedEntryCount, entryCount);
    }

    if (entryCount == 0) {
      // The segment does not have any entries, delete the file.
      final Path deleted = FileUtils.deleteFile(file);
      LOG.info("Deleted RaftLog segment since entry count is zero: startEnd={}, path={}", startEnd, deleted);
      return null;
    } else if (file.length() > segment.getTotalFileSize()) {
      // The segment has extra padding, truncate it.
      FileUtils.truncateFile(file, segment.getTotalFileSize());
    }

    try {
      segment.assertSegment(start, entryCount, corrupted, end);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read segment file " + file, e);
    }
    return segment;
  }

  private void assertSegment(long expectedStart, int expectedEntryCount, boolean corrupted, long expectedEnd) {
    Preconditions.assertSame(expectedStart, getStartIndex(), "Segment start index");
    Preconditions.assertSame(expectedEntryCount, records.size(), "Number of records");

    final long expectedLastIndex = expectedStart + expectedEntryCount - 1;
    Preconditions.assertSame(expectedLastIndex, getEndIndex(), "Segment end index");

    final LogRecord last = records.getLast();
    if (last != null) {
      Preconditions.assertSame(expectedLastIndex, last.getTermIndex().getIndex(), "Index at the last record");
      final LogRecord first = records.getFirst();
      Objects.requireNonNull(first, "first record");
      Preconditions.assertSame(expectedStart, first.getTermIndex().getIndex(), "Index at the first record");
    }
    if (!corrupted) {
      Preconditions.assertSame(expectedEnd, expectedLastIndex, "End/last Index");
    }
  }

  /**
   * The current log entry loader simply loads the whole segment into the memory.
   * In most of the cases this may be good enough considering the main use case
   * for load log entries is for leader appending to followers.
   *
   * In the future we can make the cache loader configurable if necessary.
   */
  class LogEntryLoader extends CacheLoader<LogRecord, ReferenceCountedObject<LogEntryProto>> {
    private final SegmentedRaftLogMetrics raftLogMetrics;

    LogEntryLoader(SegmentedRaftLogMetrics raftLogMetrics) {
      this.raftLogMetrics = raftLogMetrics;
    }

    @Override
    public ReferenceCountedObject<LogEntryProto> load(LogRecord key) throws IOException {
      final File file = getFile();
      // note the loading should not exceed the endIndex: it is possible that
      // the on-disk log file should be truncated but has not been done yet.
      final AtomicReference<ReferenceCountedObject<LogEntryProto>> toReturn = new AtomicReference<>();
      final LogSegmentStartEnd startEnd = LogSegmentStartEnd.valueOf(startIndex, endIndex, isOpen);
      readSegmentFile(file, startEnd, maxOpSize, getLogCorruptionPolicy(), raftLogMetrics, entryRef -> {
        final LogEntryProto entry = entryRef.retain();
        final TermIndex ti = TermIndex.valueOf(entry);
        putEntryCache(ti, entryRef, Op.LOAD_SEGMENT_FILE);
        if (ti.equals(key.getTermIndex())) {
          toReturn.set(entryRef);
        } else {
          entryRef.release();
        }
      });
      loadingTimes.incrementAndGet();
      return Objects.requireNonNull(toReturn.get());
    }
  }

  private static class Item {
    private final AtomicReference<ReferenceCountedObject<LogEntryProto>> ref;
    private final long serializedSize;

    Item(ReferenceCountedObject<LogEntryProto> obj, long serializedSize) {
      this.ref = new AtomicReference<>(obj);
      this.serializedSize = serializedSize;
    }

    ReferenceCountedObject<LogEntryProto> get() {
      return ref.get();
    }

    long release() {
      final ReferenceCountedObject<LogEntryProto> entry = ref.getAndSet(null);
      if (entry == null) {
        return 0;
      }
      entry.release();
      return serializedSize;
    }
  }

  class EntryCache {
    private Map<TermIndex, Item> map = new HashMap<>();
    private final AtomicLong size = new AtomicLong();

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + "-" + LogSegment.this;
    }

    long size() {
      return size.get();
    }

    synchronized ReferenceCountedObject<LogEntryProto> get(TermIndex ti) {
      if (map == null) {
        return null;
      }
      final Item ref = map.get(ti);
      return ref == null? null: ref.get();
    }

    /** After close(), the cache CANNOT be used again. */
    synchronized void close() {
      if (map == null) {
        return;
      }
      evict();
      map = null;
      LOG.info("Successfully closed {}", this);
    }

    /** After evict(), the cache can be used again. */
    synchronized void evict() {
      if (map == null) {
        return;
      }
      for (Iterator<Map.Entry<TermIndex, Item>> i = map.entrySet().iterator(); i.hasNext(); i.remove()) {
        release(i.next().getValue());
      }
    }

    synchronized void put(TermIndex key, ReferenceCountedObject<LogEntryProto> valueRef, Op op) {
      if (map == null) {
        return;
      }
      valueRef.retain();
      final long serializedSize = getEntrySize(valueRef.get(), op);
      release(map.put(key,  new Item(valueRef, serializedSize)));
      size.getAndAdd(serializedSize);
    }

    private void release(Item ref) {
      if (ref == null) {
        return;
      }
      final long serializedSize = ref.release();
      size.getAndAdd(-serializedSize);
    }

    synchronized void remove(TermIndex key) {
      if (map == null) {
        return;
      }
      release(map.remove(key));
    }
  }

  File getFile() {
    return LogSegmentStartEnd.valueOf(startIndex, endIndex, isOpen).getFile(storage);
  }

  private volatile boolean isOpen;
  private long totalFileSize = SegmentedRaftLogFormat.getHeaderLength();
  /** Segment start index, inclusive. */
  private final long startIndex;
  /** Segment end index, inclusive. */
  private volatile long endIndex;
  private final RaftStorage storage;
  private final SizeInBytes maxOpSize;
  private final LogEntryLoader cacheLoader;
  /** later replace it with a metric */
  private final AtomicInteger loadingTimes = new AtomicInteger();

  /**
   * the list of records is more like the index of a segment
   */
  private final Records records = new Records();
  /**
   * the entryCache caches the content of log entries.
   */
  private final EntryCache entryCache = new EntryCache();

  private LogSegment(RaftStorage storage, boolean isOpen, long start, long end, SizeInBytes maxOpSize,
      SegmentedRaftLogMetrics raftLogMetrics) {
    this.storage = storage;
    this.isOpen = isOpen;
    this.startIndex = start;
    this.endIndex = end;
    this.maxOpSize = maxOpSize;
    this.cacheLoader = new LogEntryLoader(raftLogMetrics);
  }

  long getStartIndex() {
    return startIndex;
  }

  long getEndIndex() {
    return endIndex;
  }

  boolean isOpen() {
    return isOpen;
  }

  int numOfEntries() {
    return Math.toIntExact(endIndex - startIndex + 1);
  }

  CorruptionPolicy getLogCorruptionPolicy() {
    return CorruptionPolicy.get(storage, RaftStorage::getLogCorruptionPolicy);
  }

  void appendToOpenSegment(Op op, ReferenceCountedObject<LogEntryProto> entryRef) {
    Preconditions.assertTrue(isOpen(), "The log segment %s is not open for append", this);
    append(op, entryRef, true, null);
  }

  private void append(Op op, ReferenceCountedObject<LogEntryProto> entryRef,
      boolean keepEntryInCache, Consumer<LogEntryProto> logConsumer) {
    final LogEntryProto entry = entryRef.retain();
    try {
      final LogRecord record = appendLogRecord(op, entry);
      if (keepEntryInCache) {
        putEntryCache(record.getTermIndex(), entryRef, op);
      }
      if (logConsumer != null) {
        logConsumer.accept(entry);
      }
    } finally {
      entryRef.release();
    }
  }


  private LogRecord appendLogRecord(Op op, LogEntryProto entry) {
    Objects.requireNonNull(entry, "entry == null");
    final LogRecord currentLast = records.getLast();
    if (currentLast == null) {
      Preconditions.assertTrue(entry.getIndex() == startIndex,
          "gap between start index %s and first entry to append %s",
          startIndex, entry.getIndex());
    } else {
      Preconditions.assertTrue(entry.getIndex() == currentLast.getTermIndex().getIndex() + 1,
          "gap between entries %s and %s", entry.getIndex(), currentLast.getTermIndex().getIndex());
    }

    final LogRecord record = new LogRecord(totalFileSize, entry);
    records.append(record);
    totalFileSize += getEntrySize(entry, op);
    endIndex = entry.getIndex();
    return record;
  }

  ReferenceCountedObject<LogEntryProto> getEntryFromCache(TermIndex ti) {
    return entryCache.get(ti);
  }

  /**
   * Acquire LogSegment's monitor so that there is no concurrent loading.
   */
  synchronized ReferenceCountedObject<LogEntryProto> loadCache(LogRecord record) throws RaftLogIOException {
    ReferenceCountedObject<LogEntryProto> entry = entryCache.get(record.getTermIndex());
    if (entry != null) {
      try {
        entry.retain();
        return entry;
      } catch (IllegalStateException ignored) {
        // The entry could be removed from the cache and released.
        // The exception can be safely ignored since it is the same as cache miss.
      }
    }
    try {
      return cacheLoader.load(record);
    } catch (Exception e) {
      throw new RaftLogIOException(e);
    }
  }

  LogRecord getLogRecord(long index) {
    if (index >= startIndex && index <= endIndex) {
      return records.get(index);
    }
    return null;
  }

  TermIndex getLastTermIndex() {
    final LogRecord last = records.getLast();
    return last == null ? null : last.getTermIndex();
  }

  long getTotalFileSize() {
    return totalFileSize;
  }

  long getTotalCacheSize() {
    return entryCache.size();
  }

  /**
   * Remove records from the given index (inclusive)
   */
  synchronized void truncate(long fromIndex) {
    Preconditions.assertTrue(fromIndex >= startIndex && fromIndex <= endIndex);
    for (long index = endIndex; index >= fromIndex; index--) {
      final LogRecord removed = records.removeLast();
      Preconditions.assertSame(index, removed.getTermIndex().getIndex(), "removedIndex");
      removeEntryCache(removed.getTermIndex());
      totalFileSize = removed.offset;
    }
    isOpen = false;
    this.endIndex = fromIndex - 1;
  }

  void close() {
    Preconditions.assertTrue(isOpen());
    isOpen = false;
  }

  @Override
  public String toString() {
    return isOpen() ? "log_" + "inprogress_" + startIndex :
        "log-" + startIndex + "_" + endIndex;
  }

  /** Comparator to find <code>index</code> in list of <code>LogSegment</code>s. */
  static final Comparator<Object> SEGMENT_TO_INDEX_COMPARATOR = (o1, o2) -> {
    if (o1 instanceof LogSegment && o2 instanceof Long) {
      return ((LogSegment) o1).compareTo((Long) o2);
    } else if (o1 instanceof Long && o2 instanceof LogSegment) {
      return Integer.compare(0, ((LogSegment) o2).compareTo((Long) o1));
    }
    throw new IllegalStateException("Unexpected objects to compare(" + o1 + "," + o2 + ")");
  };

  private int compareTo(Long l) {
    return (l >= getStartIndex() && l <= getEndIndex()) ? 0 :
        (this.getEndIndex() < l ? -1 : 1);
  }

  synchronized void clear() {
    records.clear();
    entryCache.close();
    endIndex = startIndex - 1;
  }

  int getLoadingTimes() {
    return loadingTimes.get();
  }

  void evictCache() {
    entryCache.evict();
  }

  void putEntryCache(TermIndex key, ReferenceCountedObject<LogEntryProto> valueRef, Op op) {
    entryCache.put(key, valueRef, op);
  }

  void removeEntryCache(TermIndex key) {
    entryCache.remove(key);
  }

  boolean hasCache() {
    return isOpen || entryCache.size() > 0; // open segment always has cache.
  }

  boolean containsIndex(long index) {
    return startIndex <= index && endIndex >= index;
  }

  boolean hasEntries() {
    return numOfEntries() > 0;
  }

}
