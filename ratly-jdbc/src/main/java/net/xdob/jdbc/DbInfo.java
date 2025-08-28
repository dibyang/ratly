package net.xdob.jdbc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DbInfo implements Serializable {
  private String name;
  private final List<DbUser> users = new ArrayList<>();
  public DbInfo() {
  }

  public DbInfo(String name) {
    this.setName(name);
  }

  public DbInfo(DbInfo dbInfo) {
    this.setName(dbInfo.getName());
    this.users.addAll(dbInfo.getUsers());
  }

  public <T extends DbInfo> T setName(String name) {
    this.name = name;
    return (T)this;
  }

  public String getName() {
    return name;
  }

  public List<DbUser> getUsers() {
    return Collections.unmodifiableList(users);
  }

  public Optional<DbUser> getUser(String user){
    return users.stream().filter(e->e.getUser().equals(user))
        .findFirst();
  }

  public DbInfo addUser(String user, String password){
    synchronized (users) {
      DbUser dbUser = users.stream().filter(e -> e.getUser().equals(user))
          .findFirst().orElse(null);
      if (dbUser == null) {
        dbUser = new DbUser(user);
        dbUser.setPassword(password);
        users.add(dbUser);
      }
    }
    return this;
  }

  @Override
  public String toString() {
    return "{" +
        "name:'" + name + '\'' +
        ", users:" + users +
        '}';
  }
}
