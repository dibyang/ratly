package net.xdob.jdbc;

public class DbDef {
  private final String db;
  private final String user;
  private final String password;

  public DbDef(String db, String user, String password) {
    this.db = db;
    this.user = user;
    this.password = password;
  }

  public String getDb() {
    return db;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }
}
