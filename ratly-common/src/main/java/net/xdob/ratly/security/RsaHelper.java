package net.xdob.ratly.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class RsaHelper {
  static Logger LOG = LoggerFactory.getLogger(RsaHelper.class);
  public static final String KEY = "ratly";

  public String encrypt(String text){
    RSAUtil rsaUtil = RSAUtil.create();
    String pub_key = getKey("/"+ KEY +".pub");
    return rsaUtil.encryptByPublicKey(pub_key, text);
  }

  public String decrypt(String text){
    RSAUtil rsaUtil = RSAUtil.create();
    String pri_key = getKey("/"+ KEY +".pri");
    return rsaUtil.decryptByPrivateKey(pri_key, text);
  }

  private String getKey(String keyPath)  {
    try {
      InputStream in = RsaHelper.class.getResourceAsStream(keyPath);
      if(in!=null) {
        StringBuilder contentBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            contentBuilder.append(line).append("\n");
          }
        } finally {
          in.close();
        }
        return contentBuilder.toString();
      }
    }catch (Exception e){
      LOG.warn("getKey fail. keyPath="+keyPath, e);
    }
    return null;
  }

  public static void main(String[] args) {
    RsaHelper rsaHelper = new RsaHelper();
    String encrypted = rsaHelper.encrypt("hhrhl2016");
    System.out.println("encrypted = " + encrypted);
  }
}
