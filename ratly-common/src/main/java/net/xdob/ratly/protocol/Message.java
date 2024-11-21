package net.xdob.ratly.protocol;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.StringUtils;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * The information clients append to the raft ring.
 */
@FunctionalInterface
public interface Message {
  static Message valueOf(ByteString bytes, Supplier<String> stringSupplier) {
    return new Message() {
      private final MemoizedSupplier<String> memoized = MemoizedSupplier.valueOf(stringSupplier);

      @Override
      public ByteString getContent() {
        return bytes;
      }

      @Override
      public String toString() {
        return memoized.get();
      }
    };
  }

  static Message valueOf(AbstractMessage abstractMessage) {
    return valueOf(abstractMessage.toByteString(), abstractMessage::toString);
  }

  static Message valueOf(ByteString bytes) {
    return valueOf(bytes, () -> "Message:" + StringUtils.bytes2HexShortString(bytes));
  }

  static Message valueOf(String string) {
    return valueOf(ByteString.copyFromUtf8(string), () -> "Message:" + string);
  }

  static int getSize(Message message) {
    return Optional.ofNullable(message).map(Message::size).orElse(0);
  }

  Message EMPTY = valueOf(ByteString.EMPTY);

  /**
   * @return the content of the message
   */
  ByteString getContent();

  default int size() {
    return Optional.ofNullable(getContent()).map(ByteString::size).orElse(0);
  }
}
