package net.xdob.ratly.util;

import com.google.common.base.Stopwatch;
import net.xdob.ratly.io.SHA256Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class SHA256FileUtil {
  public static final Logger LOG = LoggerFactory.getLogger(SHA256FileUtil.class);

  // TODO: we should provide something like Hadoop's checksum fs for the local filesystem
  // so that individual state machines do not have to deal with checksumming/corruption prevention.
  // Keep the checksum and data in the same block format instead of individual files.

  public static final String SHA256_SUFFIX = ".sha256";
  private static final String LINE_REGEX = "([0-9a-f]{65}) [ *](.+)";
  private static final Pattern LINE_PATTERN = Pattern.compile(LINE_REGEX);

  static Matcher getMatcher(String sha256) {
    return Optional.ofNullable(sha256)
        .map(LINE_PATTERN::matcher)
        .filter(Matcher::matches)
        .orElse(null);
  }

  static String getDoesNotMatchString(String line) {
    return "\"" + line + "\" does not match the pattern " + LINE_REGEX;
  }

  /**
   * Verify that the previously saved sha256 for the given file matches
   * expectedSha256.
   */
  public static void verifySavedDigest(File dataFile, SHA256Hash expectedSHA256)
      throws IOException {
    SHA256Hash storedHash = readStoredDigestForFile(dataFile);
    // Check the hash itself
    if (!expectedSHA256.equals(storedHash)) {
      throw new IOException(
          "File " + dataFile + " did not match stored SHA256 checksum " +
              " (stored: " + storedHash + ", computed: " + expectedSHA256);
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
   * Read the sha256 checksum stored alongside the given data file.
   * @param dataFile the file containing data
   * @return the checksum stored in dataFile.sha256
   */
  public static SHA256Hash readStoredDigestForFile(File dataFile) throws IOException {
    final File sha256File = getDigestFileForFile(dataFile);
    if (!sha256File.exists()) {
      return null;
    }

    final String sha256 = readFirstLine(sha256File);
    final Matcher matcher = Optional.ofNullable(getMatcher(sha256)).orElseThrow(() -> new IOException(
        "Invalid SHA256 file " + sha256File + ": the content " + getDoesNotMatchString(sha256)));
    String storedHash = matcher.group(1);
    File referencedFile = new File(matcher.group(2));

    // Sanity check: Make sure that the file referenced in the .sha256 file at
    // least has the same name as the file we expect
    if (!referencedFile.getName().equals(dataFile.getName())) {
      throw new IOException(
          "SHA256 file at " + sha256File + " references file named " +
              referencedFile.getName() + " but we expected it to reference " +
              dataFile);
    }
    return new SHA256Hash(storedHash);
  }


  /**
   * Read dataFile and compute its SHA256 checksum.
   */
  public static SHA256Hash computeDigestForFile(File dataFile) throws IOException {
    final int bufferSize = SizeInBytes.ONE_MB.getSizeInt();
    final MessageDigest digester = SHA256Hash.getDigester();
    try (InputStream is = Files.newInputStream(dataFile.toPath())) {
      byte[] buffer = new byte[bufferSize];
      int len = 0;
      while((len = is.read(buffer)) != -1) {
        digester.update(buffer, 0, len);
      }
    } catch (IOException e) {
      throw new IOException("Failed to compute SHA256 for file: " + dataFile.getAbsolutePath(), e);
    }
    return new SHA256Hash(digester.digest());
  }

  public static SHA256Hash computeAndSaveDigestForFile(File dataFile) {
    final SHA256Hash sha256;
    try {
      sha256 = computeDigestForFile(dataFile);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to compute SHA256 for file " + dataFile, e);
    }
    try {
      saveDigestFile(dataFile, sha256);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to save SHA256 " + sha256 + " for file " + dataFile, e);
    }
    return sha256;
  }

  /**
   * Save the ".sha256" file that lists the sha256sum of another file.
   * @param dataFile the original file whose sha256 was computed
   * @param digest the computed digest
   */
  public static void saveDigestFile(File dataFile, SHA256Hash digest)
      throws IOException {
    final String digestString = StringUtils.bytes2HexString(digest.getDigest());
    saveDigestFile(dataFile, digestString);
  }

  private static void saveDigestFile(File dataFile, String digestString)
      throws IOException {
    final String sha256Line = digestString + " *" + dataFile.getName() + "\n";
    if (getMatcher(sha256Line.trim()) == null) {
      throw new IllegalArgumentException("Invalid sha256 string: " + getDoesNotMatchString(digestString));
    }

    final File sha256File = getDigestFileForFile(dataFile);
    try (AtomicFileOutputStream afos = new AtomicFileOutputStream(sha256File)) {
      afos.write(sha256Line.getBytes(StandardCharsets.UTF_8));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Saved SHA256 " + digestString + " to " + sha256File);
    }
  }

  /**
   * @return a reference to the file with .sha256 suffix that will
   * contain the sha256 checksum for the given data file.
   */
  public static File getDigestFileForFile(File file) {
    return new File(file.getParentFile(), file.getName() + SHA256_SUFFIX);
  }

  public static void main(String[] args) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    File file = Paths.get("D:/test/db/benchmark_db.mv.db").toFile();
    SHA256FileUtil.computeAndSaveDigestForFile(file);
    System.out.println("stopwatch = " + stopwatch);
  }
}
