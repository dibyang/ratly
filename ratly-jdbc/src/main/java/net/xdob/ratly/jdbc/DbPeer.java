package net.xdob.ratly.jdbc;


import java.util.Objects;

/**
 * 数据库节点
 */
public class DbPeer {
  private final String id;
  private final String address;
  private boolean local;

  DbPeer(String id, String address, boolean local) {
    this.id = id;
    this.address = address;
    this.local = local;
  }

  public String getId() {
    return id;
  }

  public String getAddress() {
    return address;
  }

  public boolean isLocal() {
    return local;
  }

  public DbPeer setLocal(boolean local) {
    this.local = local;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    DbPeer dbPeer = (DbPeer) o;
    return Objects.equals(id, dbPeer.id) && Objects.equals(address, dbPeer.address);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address);
  }

  public static DbPeer.Builder newBuilder() {
    return new DbPeer.Builder();
  }

  public static DbPeer.Builder newBuilder(DbPeer peer) {
    return new DbPeer.Builder()
        .setId(peer.getId()).setAddress(peer.getAddress())
        .setLocal(peer.isLocal());
  }

  public static class Builder {
    private String id;
    private String address;
    private boolean local;

    public String getId() {
      return id;
    }

    public Builder setId(String id) {
      this.id = id;
      return this;
    }

    public String getAddress() {
      return address;
    }

    public Builder setAddress(String address) {
      this.address = address;
      return this;
    }

    public boolean isLocal() {
      return local;
    }

    public Builder setLocal(boolean local) {
      this.local = local;
      return this;
    }

    public DbPeer build(){
      return new DbPeer(Objects.requireNonNull(id, "The 'id' field is not initialized."), address, local);
    }
  }

}
