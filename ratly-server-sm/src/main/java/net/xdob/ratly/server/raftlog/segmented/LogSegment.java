
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
    LOAD_SEGMENT_FILE, // 加载日志段文件
    REMOVE_CACHE, // 移除缓存
    CHECK_SEGMENT_FILE_FULL, // 检查日志段文件是否已满
    WRITE_CACHE_WITH_STATE_MACHINE_CACHE, // 带状态机缓存写入缓存
    WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE // 不带状态机缓存写入缓存
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
    //日志序列化大小
    final int serialized = entry.getSerializedSize();
    //总大小=日志序列化大小 + 日志大小占的字节数 + 4字节校验码(CRC32)
    return serialized + CodedOutputStream.computeUInt32SizeNoTag(serialized) + 4L;
  }

  static class LogRecord {
    /** 文件中的起始偏移量 */
    private final long offset;
    /** 日志条目头（包括Term和Index） */
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
          // TODO: 使用引用计数以支持零缓存复制读取日志段文件
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
      // 该段没有条目，删除文件。
      final Path deleted = FileUtils.deleteFile(file);
      LOG.info("Deleted RaftLog segment since entry count is zero: startEnd={}, path={}", startEnd, deleted);
      return null;
    } else if (file.length() > segment.getTotalFileSize()) {
      // 该段有额外的填充，截断它。
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
   * 当前日志条目加载器简单地将整个段加载到内存中。
   * 在大多数情况下，这已经足够好，考虑到主要使用场景是领导者向跟随者追加日志。
   * 在将来，如果有必要，我们可以使缓存加载器可配置。
   */
  class LogEntryLoader extends CacheLoader<LogRecord, ReferenceCountedObject<LogEntryProto>> {
    private final SegmentedRaftLogMetrics raftLogMetrics;

    LogEntryLoader(SegmentedRaftLogMetrics raftLogMetrics) {
      this.raftLogMetrics = raftLogMetrics;
    }

    @Override
    public ReferenceCountedObject<LogEntryProto> load(LogRecord key) throws IOException {
      final File file = getFile();
      // 注意加载不应超过 endIndex: 可能磁盘上的日志文件应该被截断但尚未完成。
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

    /** 关闭后，缓存不能再使用。 */
    synchronized void close() {
      if (map == null) {
        return;
      }
      evict();
      map = null;
      LOG.info("Successfully closed {}", this);
    }

    /** 清除后，缓存可以再次使用。 */
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
  /** 段起始索引，包含。 */
  private final long startIndex;
  /** 段结束索引，包含。 */
  private volatile long endIndex;
  private final RaftStorage storage;
  private final SizeInBytes maxOpSize;
  private final LogEntryLoader cacheLoader;
  /** 后续替换为指标 */
  private final AtomicInteger loadingTimes = new AtomicInteger();

  /**
   * 记录列表更像是段的索引
   */
  private final Records records = new Records();
  /**
   * entryCache 缓存日志条目的内容。
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
   * 获取 LogSegment 的监视器，以确保没有并发加载。
   */
  synchronized ReferenceCountedObject<LogEntryProto> loadCache(LogRecord record) throws RaftLogIOException {
    ReferenceCountedObject<LogEntryProto> entry = entryCache.get(record.getTermIndex());
    if (entry != null) {
      try {
        entry.retain();
        return entry;
      } catch (IllegalStateException ignored) {
        // 条目可能已被从缓存中移除并释放。
        // 可以安全地忽略该异常，因为它与缓存未命中的情况相同。
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
   * 从给定索引（包含）开始移除记录
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

  /** 比较器用于在 <code>LogSegment</code> 列表中查找 <code>index</code>。 */
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
    return isOpen || entryCache.size() > 0; // 打开的段总是有缓存。
  }

  boolean containsIndex(long index) {
    return startIndex <= index && endIndex >= index;
  }

  boolean hasEntries() {
    return numOfEntries() > 0;
  }

}
