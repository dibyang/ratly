package net.xdob.ratly.client.impl;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import net.xdob.ratly.protocol.SerialSupport;

import java.io.*;

public class FastsImpl implements SerialSupport {

  @Override
  public byte[] asBytes(Object obj) {
    try(ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      ObjectOutputStream out = new ObjectOutputStream(bos);
      out.writeObject(obj);
      return bos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)){
      ObjectInputStream in = new ObjectInputStream(bis);
			return in.readObject();
		} catch (ClassNotFoundException|IOException e) {
			throw new RuntimeException(e);
		}
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
