package net.xdob.ratly.examples.filestore;

import com.google.protobuf.ByteString;
import net.xdob.ratly.util.ProtoUtils;
import net.xdob.ratly.util.SizeInBytes;
import net.xdob.ratly.util.TraditionalBinaryPrefix;
import net.xdob.ratly.util.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public interface FileStoreCommon {
  String STATEMACHINE_PREFIX = "example.filestore.statemachine";

  String STATEMACHINE_DIR_KEY = STATEMACHINE_PREFIX + ".dir";

  String STATEMACHINE_WRITE_THREAD_NUM = STATEMACHINE_PREFIX + ".write.thread.num";

  String STATEMACHINE_READ_THREAD_NUM = STATEMACHINE_PREFIX + ".read.thread.num";

  String STATEMACHINE_COMMIT_THREAD_NUM = STATEMACHINE_PREFIX + ".commit.thread.num";

  String STATEMACHINE_DELETE_THREAD_NUM = STATEMACHINE_PREFIX + ".delete.thread.num";

  SizeInBytes MAX_CHUNK_SIZE = SizeInBytes.valueOf(64, TraditionalBinaryPrefix.MEGA);

  static int getChunkSize(long suggestedSize) {
    return Math.toIntExact(Math.min(suggestedSize, MAX_CHUNK_SIZE.getSize()));
  }

  static ByteString toByteString(Path p) {
    return ProtoUtils.toByteString(p.toString());
  }

  static <T> CompletableFuture<T> completeExceptionally(
      long index, String message) {
    return completeExceptionally(index, message, null);
  }

  static <T> CompletableFuture<T> completeExceptionally(
      long index, String message, Throwable cause) {
    return completeExceptionally(message + ", index=" + index, cause);
  }

  static <T> CompletableFuture<T> completeExceptionally(
      String message, Throwable cause) {
    return JavaUtils.completeExceptionally(new IOException(message, cause));
  }
}
