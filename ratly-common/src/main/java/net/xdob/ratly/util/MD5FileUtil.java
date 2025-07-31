package net.xdob.ratly.util;

import com.google.common.base.Stopwatch;
import net.xdob.ratly.io.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 使用SHA256FileUtil替代
 */
public abstract class MD5FileUtil {
  public static final Logger LOG = LoggerFactory.getLogger(MD5FileUtil.class);

  // TODO: we should provide something like Hadoop's checksum fs for the local filesystem
  // so that individual state machines do not have to deal with checksumming/corruption prevention.
  // Keep the checksum and data in the same block format instead of individual files.

  public static final String MD5_SUFFIX = ".md5";
  private static final String LINE_REGEX = "([0-9a-f]{32}) [ *](.+)";
  private static final Pattern LINE_PATTERN = Pattern.compile(LINE_REGEX);

  static Matcher getMatcher(String md5) {
    return Optional.ofNullable(md5)
        .map(LINE_PATTERN::matcher)
        .filter(Matcher::matches)
        .orElse(null);
  }

  static String getDoesNotMatchString(String line) {
    return "\"" + line + "\" does not match the pattern " + LINE_REGEX;
  }

  /**
   * Verify that the previously saved md5 for the given file matches
   * expectedMd5.
   */
  public static void verifySavedDigest(File dataFile, MD5Hash expectedMD5)
      throws IOException {
    MD5Hash storedHash = readStoredDigestForFile(dataFile);
    // Check the hash itself
    if (!expectedMD5.equals(storedHash)) {
      throw new IOException(
          "File " + dataFile + " did not match stored MD5 checksum " +
              " (stored: " + storedHash + ", computed: " + expectedMD5);
    }
  }

  /** Read the first line of the given file. */
  private static String readFirstLine(File f) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        FileUtils.newInputStream(f), StandardCharsets.UTF_8))) {
      return Optional.ofNullable(reader.readLine()).map(String::trim).orElse(null);
    } catch (IOException ioe) {
      throw new IOException("Failed to read file: " + f, ioe);
    }
  }

  /**
   * Read the md5 checksum stored alongside the given data file.
   * @param dataFile the file containing data
   * @return the checksum stored in dataFile.md5
   */
  public static MD5Hash readStoredDigestForFile(File dataFile) throws IOException {
    final File md5File = getDigestFileForFile(dataFile);
    if (!md5File.exists()) {
      return null;
    }

    final String md5 = readFirstLine(md5File);
    final Matcher matcher = Optional.ofNullable(getMatcher(md5)).orElseThrow(() -> new IOException(
        "Invalid MD5 file " + md5File + ": the content " + getDoesNotMatchString(md5)));
    String storedHash = matcher.group(1);
    File referencedFile = new File(matcher.group(2));

    // Sanity check: Make sure that the file referenced in the .md5 file at
    // least has the same name as the file we expect
    if (!referencedFile.getName().equals(dataFile.getName())) {
      throw new IOException(
          "MD5 file at " + md5File + " references file named " +
              referencedFile.getName() + " but we expected it to reference " +
              dataFile);
    }
    return new MD5Hash(storedHash);
  }

  private static final Object lock = new Object();

  /**
   * Read dataFile and compute its MD5 checksum.
   */
  public static MD5Hash computeDigestForFile(File dataFile) throws IOException {
    final int bufferSize = SizeInBytes.ONE_MB.getSizeInt();
    final MessageDigest digester = MD5Hash.getDigester();
    synchronized (lock) {
      try (FileChannel in = FileUtils.newFileChannel(dataFile, StandardOpenOption.READ)) {
        final long fileSize = in.size();
        for (int offset = 0; offset < fileSize; ) {
          final int readSize = Math.toIntExact(Math.min(fileSize - offset, bufferSize));
          digester.update(in.map(FileChannel.MapMode.READ_ONLY, offset, readSize));
          offset += readSize;
        }
      }
      return new MD5Hash(digester.digest());
    }
  }

  public static MD5Hash computeAndSaveDigestForFile(File dataFile) {
    final MD5Hash md5;
    try {
      md5 = computeDigestForFile(dataFile);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to compute MD5 for file " + dataFile, e);
    }
    try {
      saveDigestFile(dataFile, md5);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to save MD5 " + md5 + " for file " + dataFile, e);
    }
    return md5;
  }

  /**
   * Save the ".md5" file that lists the md5sum of another file.
   * @param dataFile the original file whose md5 was computed
   * @param digest the computed digest
   */
  public static void saveDigestFile(File dataFile, MD5Hash digest)
      throws IOException {
    final String digestString = StringUtils.bytes2HexString(digest.getDigest());
    saveDigestFile(dataFile, digestString);
  }

  private static void saveDigestFile(File dataFile, String digestString)
      throws IOException {
    final String md5Line = digestString + " *" + dataFile.getName() + "\n";
    if (getMatcher(md5Line.trim()) == null) {
      throw new IllegalArgumentException("Invalid md5 string: " + getDoesNotMatchString(digestString));
    }

    final File md5File = getDigestFileForFile(dataFile);
    try (AtomicFileOutputStream afos = new AtomicFileOutputStream(md5File)) {
      afos.write(md5Line.getBytes(StandardCharsets.UTF_8));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Saved MD5 " + digestString + " to " + md5File);
    }
  }

  /**
   * @return a reference to the file with .md5 suffix that will
   * contain the md5 checksum for the given data file.
   */
  public static File getDigestFileForFile(File file) {
    return new File(file.getParentFile(), file.getName() + MD5_SUFFIX);
  }

  public static void main(String[] args) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    File file = Paths.get("D:/test/db/benchmark_db.mv.db").toFile();
    MD5FileUtil.computeAndSaveDigestForFile(file);
    System.out.println("stopwatch = " + stopwatch);
  }
}
