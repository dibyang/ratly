package net.xdob.ratly.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * SHA256工具封装
 */
public class SHA256Hash implements Digest {
  public static final int SHA256_LEN = 32;

  private static final ThreadLocal<MessageDigest> DIGESTER_FACTORY =
      ThreadLocal.withInitial(SHA256Hash::newDigester);

  public static MessageDigest newDigester() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Failed to create MessageDigest for SHA256", e);
    }
  }

  private byte[] digest;

  /** Constructs an SHA256Hash. */
  public SHA256Hash() {
    this.digest = new byte[SHA256_LEN];
  }

  /** Constructs an SHA256Hash from a hex string. */
  public SHA256Hash(String hex) {
    setDigest(hex);
  }

  /** Constructs an SHA256Hash with a specified value. */
  public SHA256Hash(byte[] digest) {
    if (digest.length != SHA256_LEN) {
      throw new IllegalArgumentException("Wrong length: " + digest.length);
    }
    this.digest = digest.clone();
  }

  public void readFields(DataInput in) throws IOException {
    in.readFully(digest);
  }

  /** Constructs, reads and returns an instance. */
  public static SHA256Hash read(DataInput in) throws IOException {
    SHA256Hash result = new SHA256Hash();
    result.readFields(in);
    return result;
  }

  public void write(DataOutput out) throws IOException {
    out.write(digest);
  }

  /** Copy the contents of another instance into this instance. */
  public void set(SHA256Hash that) {
    System.arraycopy(that.digest, 0, this.digest, 0, SHA256_LEN);
  }

  /** Returns the digest bytes. */
  public byte[] getDigest() {
    return digest.clone();
  }

  /** Construct a hash value for a byte array. */
  public static SHA256Hash digest(byte[] data) {
    return digest(data, 0, data.length);
  }

  /**
   * Create a thread local MD5 digester
   */
  public static MessageDigest getDigester() {
    MessageDigest digester = DIGESTER_FACTORY.get();
    digester.reset();
    return digester;
  }

  /** Construct a hash value for the content from the InputStream. */
  public static SHA256Hash digest(InputStream in) throws IOException {
    final byte[] buffer = new byte[4*1024];

    final MessageDigest digester = getDigester();
    for(int n; (n = in.read(buffer)) != -1; ) {
      digester.update(buffer, 0, n);
    }

    return new SHA256Hash(digester.digest());
  }

  /** Construct a hash value for a byte array. */
  public static SHA256Hash digest(byte[] data, int start, int len) {
    byte[] digest;
    MessageDigest digester = getDigester();
    digester.update(data, start, len);
    digest = digester.digest();
    return new SHA256Hash(digest);
  }

  /** Construct a hash value for an array of byte array. */
  public static SHA256Hash digest(byte[][] dataArr, int start, int len) {
    byte[] digest;
    MessageDigest digester = getDigester();
    for (byte[] data : dataArr) {
      digester.update(data, start, len);
    }
    digest = digester.digest();
    return new SHA256Hash(digest);
  }


  /**
   * Return a 32-bit digest of the SHA256.
   * @return the first 4 bytes of the SHA256
   */
  public int quarterDigest() {
    int value = 0;
    for (int i = 0; i < 4; i++) {
      value |= ((digest[i] & 0xff) << (8*(3-i)));
    }
    return value;
  }

  /** Returns true iff <code>o</code> is an SHA256Hash whose digest contains the
   * same values.  */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SHA256Hash)) {
      return false;
    }
    SHA256Hash other = (SHA256Hash)o;
    return Arrays.equals(this.digest, other.digest);
  }

  /** Returns a hash code value for this object.
   * Only uses the first 4 bytes, since md5s are evenly distributed.
   */
  @Override
  public int hashCode() {
    return quarterDigest();
  }

  private static final char[] HEX_DIGITS =
      {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

  /** Returns a string representation of this object. */
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder(SHA256_LEN *2);
    for (int i = 0; i < SHA256_LEN; i++) {
      int b = digest[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }
    return buf.toString();
  }

  /** Sets the digest value from a hex string. */
  public void setDigest(String hex) {
    if (hex.length() != SHA256_LEN *2) {
      throw new IllegalArgumentException("Wrong length: " + hex.length());
    }
    this.digest = new byte[SHA256_LEN];
    for (int i = 0; i < SHA256_LEN; i++) {
      int j = i << 1;
      this.digest[i] = (byte)(charToNibble(hex.charAt(j)) << 4 |
          charToNibble(hex.charAt(j+1)));
    }
  }

  private static int charToNibble(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    } else if (c >= 'a' && c <= 'f') {
      return 0xa + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
      return 0xA + (c - 'A');
    } else {
      throw new RuntimeException("Not a hex character: " + c);
    }
  }

  public static void main(String[] args) {
    SHA256Hash sha256Hash = SHA256Hash.digest("hello".getBytes());
    System.out.println("sha256Hash = " + sha256Hash);
  }
}
