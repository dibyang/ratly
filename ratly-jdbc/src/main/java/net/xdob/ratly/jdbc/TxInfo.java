package net.xdob.ratly.jdbc;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class TxInfo implements Serializable {
  private final String tx;
  private final LinkedList<Long> indexes = Lists.newLinkedList();
  private transient Session session;
  private transient volatile long accessTime = System.nanoTime();

  public TxInfo(String tx) {
    this.tx = tx;
  }

  /**
   * 更新访问时间
   */
  public void updateAccessTime(){
    accessTime = System.nanoTime();
  }

  /**
   * 获取据上次访问的时长（秒）
   * @return 据上次访问的时长（秒）
   */
  public long getAccessTimeOffset(){
    return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - accessTime);
  }

  public Session getSession() {
    return session;
  }

  public TxInfo setSession(Session session) {
    this.session = session;
    return this;
  }

  public String getTx() {
    return tx;
  }

  public LinkedList<Long> getIndexes() {
    return indexes;
  }

  public byte[] getIndexesBytes(){
    if(indexes.isEmpty()){
      return new byte[0];
    }
    ByteBuffer buffer = ByteBuffer.allocate(8 * indexes.size());
    for (int i = 0; i < indexes.size(); i++) {
      buffer.putLong(i, indexes.get(i));
    }
    return buffer.array();
  }

  public void setIndexes(byte[] bytes){
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int count = bytes.length / 8;
    for (int i = 0; i < count; i++) {
      indexes.add(i, buffer.getLong(i));
    }
  }


}
