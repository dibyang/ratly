package net.xdob.ratly.protocol;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/** Unique identifier implemented using {@link UUID}. */
public abstract class RaftId {
  static final String ZERO_ID = "";




  abstract static class Factory<ID extends RaftId> {
    private final Cache<String, ID> cache = CacheBuilder.newBuilder()
        .weakValues()
        .build();

    abstract ID newInstance(String id);

    final ID valueOf(String id) {
      try {
        return cache.get(id, () -> newInstance(id));
      } catch (ExecutionException e) {
        throw new IllegalStateException("Failed to valueOf(" + id + ")", e);
      }
    }

    final ID valueOf(ByteString bytes) {
      return bytes != null? valueOf(bytes.toStringUtf8()): emptyId();
    }

    ID emptyId() {
      return valueOf(ZERO_ID);
    }

    ID randomId() {
      return valueOf(UUID.randomUUID().toString());
    }
  }

  private final String id;
  private final Supplier<ByteString> idBytes;

  RaftId(String id) {
    this.id = Preconditions.assertNotNull(id, "id");
    this.idBytes = JavaUtils.memoize(() -> ByteString.copyFrom(id, StandardCharsets.UTF_8));

  }


  public ByteString toByteString() {
    return idBytes.get();
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public boolean equals(Object other) {
    return other == this ||
        (other instanceof RaftId
            && this.getClass() == other.getClass()
            && id.equals(((RaftId) other).id));
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  public String getId() {
    return id;
  }
}
