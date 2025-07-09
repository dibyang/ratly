package net.xdob.ratly.jdbc.sql;

import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.util.Finder;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcConnectionInfo {
  public static final String PASSWORD = "password";
  public static final String USER = "user";
  private final String db;
  private final String group;
  private final String url;
  private final Properties info;
  private final List<RaftPeer> peers = new ArrayList<>();

  /**
   * jdbc:ratly:{db}:group={group};peers={peers}
   * @param url
   * @param info
   */
  public JdbcConnectionInfo(String url, Properties info) {this.url = url;
    this.info = info;
    Finder finder = Finder.c(url).tail(":", 1);
    db = finder.head(":").getValue();
    Map<String, String> config = getConfig(finder.tail(":").getValue());
    group = config.get("group");
    peers.addAll(parsePeers(config.get("peers")));
  }

  private static Map<String, String> getConfig(String values) {
    Map<String,String> config = new HashMap<>();
    String[] parts = values.split(";");
    for (String part : parts) {
      if(part!=null&&!part.isEmpty()) {
        Finder c = Finder.c(part);
        String n = c.head("=").getValue();
        String v = c.tail("=").getValue();
        config.put(n, v);
      }
    }
    return config;
  }

  private List<RaftPeer> parsePeers(String peers) {
    return Stream.of(peers.split(",")).map(address -> {
      String[] addressParts = address.split(":");
      if (addressParts.length < 3) {
        throw new IllegalArgumentException(
            "Raft peer " + address + " is not a legitimate format. "
                + "(format: name:host:port:dataStreamPort:clientPort:adminPort)");
      }

      RaftPeer.Builder builder = RaftPeer.newBuilder();
      builder.setId(addressParts[0]).setAddress(addressParts[1] + ":" + addressParts[2]);
      if (addressParts.length >= 4) {
        builder.setDataStreamAddress(addressParts[1] + ":" + addressParts[3]);
        if (addressParts.length >= 5) {
          builder.setClientAddress(addressParts[1] + ":" + addressParts[4]);
          if (addressParts.length >= 6) {
            builder.setAdminAddress(addressParts[1] + ":" + addressParts[5]);
          }
        }
      }
      return builder.build();
    }).collect(Collectors.toList());
  }

  public String getDb() {
    return db;
  }

  public String getGroup() {
    return group;
  }

  public String getUrl() {
    return url;
  }

  public Properties getInfo() {
    return info;
  }

  public List<RaftPeer> getPeers() {
    return peers;
  }

  public String getUser(){
    return info.getProperty(USER);
  }

  public String getPassword(){
    return info.getProperty(PASSWORD);
  }

}
