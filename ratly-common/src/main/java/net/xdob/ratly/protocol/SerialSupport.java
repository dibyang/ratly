package net.xdob.ratly.protocol;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;

public interface SerialSupport {
  byte[] asBytes(Object obj);
  ByteString asByteString(Object obj);
  Object asObject(byte[] bytes);
  Object asObject(ByteString byteString);
  Object asObject(AbstractMessage msg);
  <T> T as(byte[] bytes);
  <T> T as(ByteString byteString);
  <T> T as(AbstractMessage msg);
}
