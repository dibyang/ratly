package net.xdob.ratly.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * 签名辅助工具类
 */
public class SignHelper {
  public static final String SIGNER = "ratly";

  static Logger LOG = LoggerFactory.getLogger(SignHelper.class);

  public String getSigner(){
    return SIGNER;
  }

  public String sign(String code4Sign ){
    return doSign(code4Sign);
  }

  public boolean verifySign(String code4Sign, String sign){
    RSAUtil rsaUtil = RSAUtil.create();
    String pub_key = getKey("/"+SIGNER+".pub");
    return rsaUtil.verifySHA256WithRSA(pub_key, code4Sign, sign);
  }

  private String doSign(String text){
    RSAUtil rsaUtil = RSAUtil.create();
    String pri_key = getKey("/"+SIGNER+".pri");
    return rsaUtil.signSha256withRSA(pri_key, text);
  }


  private String getKey(String keyPath)  {
    try {
      InputStream in = SignHelper.class.getResourceAsStream(keyPath);
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


}
