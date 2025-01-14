package net.xdob.ratly.client.impl;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;
import net.xdob.ratly.protocol.SerialSupport;

public class FastsImpl implements SerialSupport {
  private final FSTConfiguration fasts = FSTConfiguration.createDefaultConfiguration();

  @Override
  public byte[] asBytes(Object obj) {
    return fasts.asByteArray(obj);
  }

  @Override
  public ByteString asByteString(Object obj) {
    return ByteString.copyFrom(asBytes(obj));
  }

  @Override
  public Object asObject(byte[] bytes) {
    if(bytes==null||bytes.length==0){
      return null;
    }
    return fasts.asObject(bytes);
  }

  @Override
  public Object asObject(ByteString byteString) {
    if(byteString==null||byteString.isEmpty()){
      return null;
    }
    return asObject(byteString.toByteArray());
  }

  @Override
  public Object asObject(AbstractMessage msg) {
    return asObject(msg.toByteArray());
  }

  @Override
  public <T> T as(byte[] bytes) {
    return (T)asObject(bytes);
  }

  @Override
  public <T> T as(ByteString byteString) {
    return (T)asObject(byteString);
  }

  @Override
  public <T> T as(AbstractMessage msg) {
    return (T)asObject(msg);
  }
}
