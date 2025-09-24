package net.xdob.ratly.jdbc;

import java.io.Serializable;

public class DbUser implements Serializable {
  private String user;
  private String password;

  public DbUser() {
  }

  public DbUser(String user) {
    this.user = user;
  }

  public DbUser setUser(String user) {
    this.user = user;
    return this;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public DbUser setPassword(String password) {
    this.password = password;
    return this;
  }

  @Override
  public String toString() {
    return "{" +
        "user:'" + user + '\'' +
        ", password:'" + password + '\'' +
        '}';
  }
}
