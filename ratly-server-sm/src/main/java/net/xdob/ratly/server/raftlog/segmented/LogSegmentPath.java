package net.xdob.ratly.server.raftlog.segmented;

import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;

/**
 * {@link LogSegmentStartEnd} with a {@link Path}.
 * <p>
 * 这是一个基于值的类。
 */
public final class LogSegmentPath {
  static final Logger LOG = LoggerFactory.getLogger(LogSegmentPath.class);

  private static final Comparator<LogSegmentPath> COMPARATOR =
      Comparator.comparing(LogSegmentPath::getStartEnd);

  private final Path path;
  private final LogSegmentStartEnd startEnd;

  private LogSegmentPath(Path path, long startIndex, Long endIndex) {
    this.path = path;
    this.startEnd = LogSegmentStartEnd.valueOf(startIndex, endIndex);
  }

  public Path getPath() {
    return path;
  }

  public LogSegmentStartEnd getStartEnd() {
    return startEnd;
  }

  @Override
  public String toString() {
    return path+ "(" + startEnd + ")";
  }

  /**
   * Get a list of {@link LogSegmentPath} from the given storage.
   *
   * @return a list of log segment paths sorted by the indices.
   */
  public static List<LogSegmentPath> getLogSegmentPaths(RaftStorage storage) throws IOException {
    return getLogSegmentPaths(storage.getStorageDir().getCurrentDir().toPath());
  }

  private static List<LogSegmentPath> getLogSegmentPaths(Path dir) throws IOException {
    final List<LogSegmentPath> list = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        Optional.ofNullable(matchLogSegment(path)).ifPresent(list::add);
      }
    }
    list.sort(COMPARATOR);
    return list;
  }

  /**
   * Match the given path with the {@link LogSegmentStartEnd#getClosedSegmentPattern()}
   * or the {@link LogSegmentStartEnd#getOpenSegmentPattern()}.
   *
   * Note that if the path is a zero size open segment, this method will try to delete it.
   *
   * @return the log segment file matching the given path.
   */
  public static LogSegmentPath matchLogSegment(Path path) {
    return Optional.ofNullable(matchCloseSegment(path)).orElseGet(() -> matchOpenSegment(path));
  }

  private static LogSegmentPath matchCloseSegment(Path path) {
    final String fileName = String.valueOf(Objects.requireNonNull(path).getFileName());
    final Matcher matcher = LogSegmentStartEnd.getClosedSegmentPattern().matcher(fileName);
    if (matcher.matches()) {
      Preconditions.assertTrue(matcher.groupCount() == 2);
      return newInstance(path, matcher.group(1), matcher.group(2));
    }
    return null;
  }

  private static LogSegmentPath matchOpenSegment(Path path) {
    final String fileName = String.valueOf(Objects.requireNonNull(path).getFileName());
    final Matcher matcher = LogSegmentStartEnd.getOpenSegmentPattern().matcher(fileName);
    if (matcher.matches()) {
      if (path.toFile().length() > 0L) {
        return newInstance(path, matcher.group(1), null);
      }

      LOG.info("Found zero size open segment file " + path);
      try {
        Files.delete(path);
        LOG.info("Deleted zero size open segment file " + path);
      } catch (IOException e) {
        LOG.warn("Failed to delete zero size open segment file " + path + ": " + e);
      }
    }
    return null;
  }

  private static LogSegmentPath newInstance(Path path, String startIndexString, String endIndexString) {
    final long startIndex = Long.parseLong(startIndexString);
    final Long endIndex = Optional.ofNullable(endIndexString).map(Long::parseLong).orElse(null);
    return new LogSegmentPath(path, startIndex, endIndex);
  }
}