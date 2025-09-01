package net.xdob.ratly.client.impl;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import net.xdob.ratly.protocol.SerialSupport;
import net.xdob.ratly.util.ProtoUtils;

import java.io.*;

public class FastsImpl implements SerialSupport {

  @Override
  public byte[] asBytes(Object obj) {
		return asByteString(obj).toByteArray();
  }

  @Override
  public ByteString asByteString(Object obj) {
    return ProtoUtils.writeObject2ByteString(obj);
  }

  @Override
  public Object asObject(byte[] bytes) {
    if(bytes==null||bytes.length==0){
      return null;
    }
		return asObject(ByteString.copyFrom(bytes));
	}

  @Override
  public Object asObject(ByteString byteString) {
    if(byteString==null||byteString.isEmpty()){
      return null;
    }
    return ProtoUtils.toObject(byteString);
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
