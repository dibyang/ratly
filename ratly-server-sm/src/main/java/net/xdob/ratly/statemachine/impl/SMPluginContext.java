package net.xdob.ratly.statemachine.impl;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.SerialSupport;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.statemachine.RaftLogQuery;
import net.xdob.ratly.statemachine.SnapshotInfo;

import java.util.concurrent.ScheduledExecutorService;

public interface SMPluginContext {
  RaftPeerId getPeerId();
  ScheduledExecutorService getScheduler();
  SnapshotInfo getLatestSnapshot();
  SerialSupport getFasts();
  Object asObject(byte[] bytes);
  Object asObject(ByteString byteString);
  ByteString getByteString(Object value);
  Object asObject(AbstractMessage msg);
  <T> T as(byte[] bytes);
  <T> T as(ByteString byteString);
  <T> T as(AbstractMessage msg);
  RaftLogQuery getRaftLogQuery();
  boolean isLeader();
  PasswordEncoder getPasswordEncoder();
}
