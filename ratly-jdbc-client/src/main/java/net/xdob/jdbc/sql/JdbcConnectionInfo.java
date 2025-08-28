package net.xdob.jdbc.sql;

import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.util.Finder;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcConnectionInfo {
  public static final String PASSWORD = "password";
  public static final String USER = "user";
  public static final int DEFAULT_PORT = 7800;
  public static final String START_URL = "jdbc:ratly:";
  private final String db;
	private final String group;
  private final String url;
  private final Properties info;
  private final List<RaftPeer> peers = new ArrayList<>();
  private final int port;
  private final int vport;

  /**
   * jdbc:ratly:{host}/{database}?port={port}
   * ex: jdbc:ratly:13.13.163.1$n1@13.13.14.163,n2@13.13.14.164/b_db?port=7800
   *     jdbc:ratly:single$n1@127.0.0.1/b_db?port=7800
   * @param url
   * @param info
   */
  public JdbcConnectionInfo(String url, Properties info) {this.url = url;
    this.info = info;
    Finder finder = Finder.c(url).tail(START_URL);
    if(finder.getValue()==null){
      throw new IllegalArgumentException(
          "url " + url + " is not a legitimate format. "
              + "(format: jdbc:ratly:{host}/{database}?port={port})");
    }
    db = finder.tail("/").head("?", e-> e).getValue();
    if(db==null||db.isEmpty()){
      throw new IllegalArgumentException(
          "url " + url + " is not a legitimate format. "
              + "(format: jdbc:ratly:{host}/{database}?port={port})");
    }
    String host = finder.head("/").getValue();
    if(host==null||host.isEmpty()){
      throw new IllegalArgumentException(
          "host " + host + " is not a legitimate format. "
              + "(format: group$nodes)");
    }
    group = Finder.c(host).head("$").getValue();
    if(group==null){
      throw new IllegalArgumentException(
          "host " + host + " is not a legitimate format. "
              + "(format: group$nodes)");
    }
    String nodes = Finder.c(host).tail("$").getValue();
    if(nodes==null||nodes.isEmpty()){
      throw new IllegalArgumentException(
          "host " + host + " is not a legitimate format. "
              + "(format: group$nodes)");
    }

    Map<String, String> config = getConfig(finder.tail("?", e-> e).getValue());

    port = Optional.ofNullable(config.get("port"))
        .map(Integer::parseInt)
        .orElse(DEFAULT_PORT);
    vport =  Optional.ofNullable(config.get("vport"))
        .map(Integer::parseInt)
        .orElse(
            Optional.ofNullable(info.getProperty("vport"))
              .map(Integer::parseInt)
              .orElse(port+1));
    List<RaftPeer> nodeList = parsePeers(nodes, port);
    peers.addAll(nodeList);
    if(nodeList.size()%2==0){
      for (RaftPeer node : nodeList) {
        String address = node.getAddress();
        peers.add(RaftPeer.newBuilder()
            .setId("vn_"+node.getId())
            .setAddress(Finder.c(address).last().head(":").getValue()+":"+(vport))
            .build());
      }
    }
  }


  private static Map<String, String> getConfig(String values) {
    Map<String,String> config = new HashMap<>();
    String[] parts = values.split("&");
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

  private List<RaftPeer> parsePeers(String peers,int port) {
    return Stream.of(peers.split(",")).map(address -> {
      String[] addressParts = address.split("@");
      if (addressParts.length < 2) {
        throw new IllegalArgumentException(
            "Raft peers " + peers + " is not a legitimate format. "
                + "(format: name:host)");
      }

      String id = addressParts[0];
      RaftPeer.Builder builder = RaftPeer.newBuilder();
      builder.setId(id).setAddress(addressParts[1] + ":" + port);
      return builder.build();
    }).filter(e->!e.isVirtual())
        .collect(Collectors.toList());
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


  public static void main(String[] args) {
    List<String> urls= new ArrayList<>();
    urls.add("jdbc:ratly:single$n1@127.0.0.1/b_db");
    urls.add("jdbc:ratly:single#$n1@127.0.0.1/b_db?port=7802");
    urls.add("jdbc:ratly:13.13.163.1$n1@13.13.14.163,n2@13.13.14.164/b_db?port=7802&vport=7808");
    for (String url : urls) {
      System.out.println("url = " + url);
      JdbcConnectionInfo info = new JdbcConnectionInfo(url, new Properties());
      System.out.println("info.group = " + info.group);
      System.out.println("info.peers = " + info.peers);
      System.out.println("info.db = " + info.db);
    }
  }
}
