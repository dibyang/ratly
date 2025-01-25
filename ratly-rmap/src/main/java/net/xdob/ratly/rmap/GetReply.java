package net.xdob.ratly.rmap;

public class GetReply {
  private int size;
  private Object data;
  private Exception ex;

  public int getSize() {
    return size;
  }

  public GetReply setSize(int size) {
    this.size = size;
    return this;
  }

  public <R> R getData() {
    return (R)data;
  }

  public <R> GetReply setData(R data) {
    this.data = data;
    return this;
  }

  public Exception getEx() {
    return ex;
  }

  public GetReply setEx(Exception ex) {
    this.ex = ex;
    return this;
  }

  public boolean hasEx(){
    return ex!=null;
  }
}
