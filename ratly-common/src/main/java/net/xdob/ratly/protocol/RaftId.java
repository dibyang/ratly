
package net.xdob.ratly.protocol;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/** Unique identifier implemented using {@link UUID}. */
public abstract class RaftId {
  static final UUID ZERO_UUID = new UUID(0L, 0L);
  static final ByteString ZERO_UUID_BYTESTRING = toByteString(ZERO_UUID);
  private static final int BYTE_LENGTH = 16;

  static UUID toUuid(ByteString bytes) {
    Objects.requireNonNull(bytes, "bytes == null");
    Preconditions.assertSame(BYTE_LENGTH, bytes.size(), "bytes.size()");
    final ByteBuffer buf = bytes.asReadOnlyByteBuffer();
    return new UUID(buf.getLong(), buf.getLong());
  }

  static ByteString toByteString(UUID uuid) {
    Objects.requireNonNull(uuid, "uuid == null");
    final byte[] array = new byte[BYTE_LENGTH];
    ByteBuffer.wrap(array)
        .putLong(uuid.getMostSignificantBits())
        .putLong(uuid.getLeastSignificantBits());
    return UnsafeByteOperations.unsafeWrap(array);
  }

  abstract static class Factory<ID extends RaftId> {
    private final Cache<UUID, ID> cache = CacheBuilder.newBuilder()
        .weakValues()
        .build();

    abstract ID newInstance(UUID uuid);

    final ID valueOf(UUID uuid) {
      try {
        return cache.get(uuid, () -> newInstance(uuid));
      } catch (ExecutionException e) {
        throw new IllegalStateException("Failed to valueOf(" + uuid + ")", e);
      }
    }

    final ID valueOf(ByteString bytes) {
      return bytes != null? valueOf(toUuid(bytes)): emptyId();
    }

    ID emptyId() {
      return valueOf(ZERO_UUID);
    }

    ID randomId() {
      return valueOf(UUID.randomUUID());
    }
  }

  private final UUID uuid;
  private final Supplier<ByteString> uuidBytes;
  private final Supplier<String> uuidString;

  RaftId(UUID uuid) {
    this.uuid = Preconditions.assertNotNull(uuid, "uuid");
    this.uuidBytes = JavaUtils.memoize(() -> toByteString(uuid));
    this.uuidString = JavaUtils.memoize(() -> createUuidString(uuid));
    Preconditions.assertTrue(ZERO_UUID == uuid || !uuid.equals(ZERO_UUID),
        () -> "Failed to create " + JavaUtils.getClassSimpleName(getClass()) + ": UUID " + ZERO_UUID + " is reserved.");
  }

  /** @return the last 12 hex digits. */
  String createUuidString(UUID id) {
    final String s = id.toString().toUpperCase();
    final int i = s.lastIndexOf('-');
    return s.substring(i + 1);
  }

  public ByteString toByteString() {
    return uuidBytes.get();
  }

  @Override
  public String toString() {
    return uuidString.get();
  }

  @Override
  public boolean equals(Object other) {
    return other == this ||
        (other instanceof RaftId
            && this.getClass() == other.getClass()
            && uuid.equals(((RaftId) other).uuid));
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  public UUID getUuid() {
    return uuid;
  }
}
