package net.xdob.ratly.security;

import java.util.Arrays;
import java.util.UUID;

public abstract class Base58 {
  // Bsae58 编码表
  public static final char[] ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".toCharArray();
  private static final char ENCODED_ZERO = ALPHABET[0];
  private static final int[] INDEXES = new int[128];
  static {
    Arrays.fill(INDEXES, -1);
    for (int i = 0; i < ALPHABET.length; i++) {
      INDEXES[ALPHABET[i]] = i;
    }
  }

  public static String encode(UUID uuid) {
    return encode(uuid2Bytes(uuid));
  }

  public static String encode(long value) {
    return encode(long2Bytes(value));
  }

  public static byte[] long2Bytes(long value) {
    byte[] byteNum = new byte[8];

    for(int ix = 0; ix < 8; ++ix) {
      int offset = 64 - (ix + 1) * 8;
      byteNum[ix] = (byte)((int)(value >> offset & 255L));
    }

    return byteNum;
  }

  public static byte[] uuid2Bytes(UUID uuid) {
    byte[] byteNum = new byte[16];
    byte[] mostSigBits = long2Bytes(uuid.getMostSignificantBits());
    byte[] leastSigBits = long2Bytes(uuid.getLeastSignificantBits());

    for(int ix = 0; ix < 8; ++ix) {
      byteNum[ix] = mostSigBits[ix];
      byteNum[8 + ix] = leastSigBits[ix];
    }

    return byteNum;
  }

  public static long bytes2Long(byte[] bytes) {
    long num = 0L;
    if (bytes.length == 8) {
      for(int ix = 0; ix < 8; ++ix) {
        num <<= 8;
        num |= (long)(bytes[ix] & 255);
      }
    }else{
      throw new IllegalArgumentException("bytes length not eq 8");
    }
    return num;
  }

  public static UUID bytes2Uuid(byte[] bytes) {
    UUID uuid = null;
    if (bytes.length == 16) {
      byte[] mostBytes = new byte[8];
      byte[] leastBytes = new byte[8];

      for(int ix = 0; ix < 8; ++ix) {
        mostBytes[ix] = bytes[ix];
        leastBytes[ix] = bytes[ix + 8];
      }

      long mostSigBits = bytes2Long(mostBytes);
      long leastSigBits = bytes2Long(leastBytes);
      uuid = new UUID(mostSigBits, leastSigBits);
    }else{
      throw new IllegalArgumentException("bytes length not eq 16");
    }
    return uuid;
  }

  // Base58 编码
  public static String encode(byte[] input) {
    if (input.length == 0) {
      return "";
    }
    // 统计前导0
    int zeros = 0;
    while (zeros < input.length && input[zeros] == 0) {
      ++zeros;
    }
    // 复制一份进行修改
    input = Arrays.copyOf(input, input.length);
    // 最大编码数据长度
    char[] encoded = new char[input.length * 2];
    int outputStart = encoded.length;
    // Base58编码正式开始
    for (int inputStart = zeros; inputStart < input.length;) {
      encoded[--outputStart] = ALPHABET[divmod(input, inputStart, 256, 58)];
      if (input[inputStart] == 0) {
        ++inputStart;
      }
    }
    // 输出结果中有0,去掉输出结果的前端0
    while (outputStart < encoded.length && encoded[outputStart] == ENCODED_ZERO) {
      ++outputStart;
    }
    // 处理前导0
    while (--zeros >= 0) {
      encoded[--outputStart] = ENCODED_ZERO;
    }
    // 返回Base58
    return new String(encoded, outputStart, encoded.length - outputStart);
  }

  public static byte[] decode(String input) {
    if (input.length() == 0) {
      return new byte[0];
    }
    // 将BASE58编码的ASCII字符转换为BASE58字节序列
    byte[] input58 = new byte[input.length()];
    for (int i = 0; i < input.length(); ++i) {
      char c = input.charAt(i);
      int digit = c < 128 ? INDEXES[c] : -1;
      if (digit < 0) {
        String msg = "Invalid characters,c=" + c;
        throw new RuntimeException(msg);
      }
      input58[i] = (byte) digit;
    }
    // 统计前导0
    int zeros = 0;
    while (zeros < input58.length && input58[zeros] == 0) {
      ++zeros;
    }
    // Base58 编码转 字节序（256进制）编码
    byte[] decoded = new byte[input.length()];
    int outputStart = decoded.length;
    for (int inputStart = zeros; inputStart < input58.length;) {
      decoded[--outputStart] = divmod(input58, inputStart, 58, 256);
      if (input58[inputStart] == 0) {
        ++inputStart;
      }
    }
    // 忽略在计算过程中添加的额外超前零点。
    while (outputStart < decoded.length && decoded[outputStart] == 0) {
      ++outputStart;
    }
    // 返回原始的字节数据
    return Arrays.copyOfRange(decoded, outputStart - zeros, decoded.length);
  }

  // 进制转换代码
  private static byte divmod(byte[] number, int firstDigit, int base, int divisor) {
    int remainder = 0;
    for (int i = firstDigit; i < number.length; i++) {
      int digit = (int) number[i] & 0xFF;
      int temp = remainder * base + digit;
      number[i] = (byte) (temp / divisor);
      remainder = temp % divisor;
    }
    return (byte) remainder;
  }



}
