package net.xdob.ratly.rmap;

public class PutReply {
  private int count;
  private Object data;
  private Exception ex;

  public int getCount() {
    return count;
  }

  public PutReply setCount(int count) {
    this.count = count;
    return this;
  }

  public Exception getEx() {
    return ex;
  }

  public PutReply setEx(Exception ex) {
    this.ex = ex;
    return this;
  }

  public boolean hasEx(){
    return ex!=null;
  }

  public <R> R getData() {
    return (R)data;
  }

  public <R> PutReply setData(R data) {
    this.data = data;
    return this;
  }
}
