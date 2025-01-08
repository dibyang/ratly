package net.xdob.ratly.statemachine.impl;

import com.google.protobuf.ByteString;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;
import net.xdob.ratly.server.protocol.TermIndex;

import java.util.concurrent.ScheduledExecutorService;

public interface SMPluginContext {
  boolean updateLastAppliedTermIndex2(long term, long index);
  boolean updateLastAppliedTermIndex2(TermIndex termIndex);
  ScheduledExecutorService getScheduler();
  FSTConfiguration getFasts();
  Object asObject(byte[] bytes);
  Object asObject(ByteString byteString);
  ByteString getByteString(Object value);
}
