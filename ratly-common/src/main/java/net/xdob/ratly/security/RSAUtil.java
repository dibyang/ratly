package net.xdob.ratly.security;

import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

public class RSAUtil {
  static final Logger LOG = LoggerFactory.getLogger(RSAUtil.class);
  static final String BEGIN_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----";
  static final String END_PUBLIC_KEY = "-----END PUBLIC KEY-----";
  static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
  static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";

  public static final String RSA_ECB_PKCS1PADDING = "RSA/ECB/PKCS1PADDING";
  private final SecureRandom secureRandom = new SecureRandom();
  public Cipher rsaCipher;
  public static final String RSA = "RSA";

  private RSAUtil() {
  }

  public static RSAUtil create() {
    return new RSAUtil();
  }

  public KeyPair generateKeyPair() {
    return this.generateKeyPair(2048);
  }

  public KeyPair generateKeyPair(int keySize) {
    KeyPair keyPair = null;

    try {
      KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
      keyPairGen.initialize(keySize);
      keyPair = keyPairGen.generateKeyPair();
    } catch (Exception e) {
      System.err.println("Exception:" + e.getMessage());
    }

    return keyPair;
  }

  public int getKeySize(Key key) {
    if (key instanceof PrivateKey) {
      return this.getKeySize((PrivateKey)key);
    } else {
      return key instanceof PublicKey ? this.getKeySize((PublicKey)key) : 0;
    }
  }

  public int getKeySize(PrivateKey key) {
    String algorithm = key.getAlgorithm();
    BigInteger prime = null;
    if ("RSA".equals(algorithm)) {
      RSAPrivateKey keySpec = (RSAPrivateKey)key;
      prime = keySpec.getModulus();
    }
    assert prime != null;
    return prime.toString(2).length();
  }

  public int getKeySize(PublicKey key) {
    String algorithm = key.getAlgorithm();
    BigInteger prime = null;
    if ("RSA".equals(algorithm)) {
      RSAPublicKey keySpec = (RSAPublicKey)key;
      prime = keySpec.getModulus();
    }
    assert prime != null;
    return prime.toString(2).length();
  }

  public PublicKey getPublicKey(String key) throws NoSuchAlgorithmException, InvalidKeySpecException {
    key = this.getKey(key, BEGIN_PUBLIC_KEY, END_PUBLIC_KEY);
    byte[] keyBytes = BaseEncoding.base64().decode(key);
    X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    return keyFactory.generatePublic(keySpec);
  }

  private String getKey(String key, String begin, String end) {
    if (key.startsWith(begin)) {
      key = key.substring(begin.length());
    }

    int index = key.indexOf(end);
    if (index > 0) {
      key = key.substring(0, index);
    }

    key = key.replaceAll("\n", "").replaceAll("\r", "");
    return key;
  }

  public PrivateKey getPrivateKey(String key) throws NoSuchAlgorithmException, InvalidKeySpecException {
    key = this.getKey(key, BEGIN_PRIVATE_KEY, END_PRIVATE_KEY);
    byte[] keyBytes = BaseEncoding.base64().decode(key);
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    return keyFactory.generatePrivate(keySpec);
  }

  public String getKeyString(Key key) {
    byte[] keyBytes = key.getEncoded();
    return BaseEncoding.base64().encode(keyBytes);
  }

  public String getPublicKeyWithBase64(KeyPair keyPair) {
    byte[] publicKey = keyPair.getPublic().getEncoded();
    return BaseEncoding.base64().encode(publicKey);
  }

  public String getPrivateKeyWithBase64(KeyPair keyPair) {
    byte[] privateKey = keyPair.getPrivate().getEncoded();
    return BaseEncoding.base64().encode(privateKey);
  }

  public String encryptByPublicKey(String key, String data) {
    try {
      PublicKey publicKey = this.getPublicKey(key);
      return this.encode(publicKey, (String)data);
    } catch (Exception e) {
      LOG.warn("", e);
      return null;
    }
  }

  public String decryptByPrivateKey(String key, String data) {
    try {
      PrivateKey privateKey = this.getPrivateKey(key);
      return this.decode(privateKey, (String)data);
    } catch (Exception e) {
      LOG.warn("", e);
      return null;
    }
  }

  public String encode(Key key, String content) throws NoSuchPaddingException, IOException {
    byte[] data = content.getBytes(StandardCharsets.UTF_8);
    return BaseEncoding.base64().encode(this.encode(key, data));
  }

  public byte[] encode(Key key, byte[] data) throws NoSuchPaddingException, IOException {
    try {
      this.rsaCipher = Cipher.getInstance(RSA_ECB_PKCS1PADDING);
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("", e);
    }

    try {
      this.rsaCipher.init(1, key, this.secureRandom);
      return this.rsaCipher.doFinal(data);
    } catch (InvalidKeyException e) {
      throw new IOException("InvalidKey", e);
    } catch (IllegalBlockSizeException e) {
      throw new IOException("IllegalBlockSize", e);
    } catch (BadPaddingException e) {
      throw new IOException("BadPadding", e);
    }
  }

  public String decode(Key key, String content) throws NoSuchPaddingException, IOException {
    byte[] data = null;

    try {
      data = BaseEncoding.base64().decode(content);
    } catch (Exception e1) {
      LOG.warn("", e1);
    }

    return new String(this.decode(key, data), StandardCharsets.UTF_8);
  }

  public byte[] decode(Key key, byte[] data) throws NoSuchPaddingException, IOException {
    try {
      this.rsaCipher = Cipher.getInstance("RSA/ECB/PKCS1PADDING");
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("", e);
    }

    try {
      this.rsaCipher.init(2, key, this.secureRandom);
      return this.rsaCipher.doFinal(data);
    } catch (InvalidKeyException e) {
      throw new IOException("InvalidKey", e);
    } catch (IllegalBlockSizeException e) {
      throw new IOException("IllegalBlockSize", e);
    } catch (BadPaddingException e) {
      throw new IOException("BadPadding", e);
    }
  }

  public String signMD5withRSA(String privateKeyStr, String data) {
    return this.sign("MD5withRSA", privateKeyStr, data);
  }

  public boolean verifyMD5withRSA(String publicKeyStr, String data, String sign) {
    return this.verify("MD5withRSA", publicKeyStr, data, sign);
  }

  public String signSha256withRSA(String privateKeyStr, String data) {
    return this.sign("SHA256withRSA", privateKeyStr, data);
  }

  public String sign(String algorithm, String privateKeyStr, String data) {
    try {
      Signature Sign = Signature.getInstance(algorithm);
      PrivateKey privateKey = this.getPrivateKey(privateKeyStr);
      Sign.initSign(privateKey);
      Sign.update(data.getBytes());
      byte[] signed = Sign.sign();
      return BaseEncoding.base64().encode(signed);
    } catch (Exception e) {
      LOG.warn("", e);
      return null;
    }
  }

  public boolean verifySHA256WithRSA(String publicKeyStr, String data, String sign) {
    return this.verify("SHA256withRSA", publicKeyStr, data, sign);
  }

  public boolean verify(String algorithm, String publicKeyStr, String data, String sign) {
    try {
      PublicKey publicKey = this.getPublicKey(publicKeyStr);
      Signature sig = Signature.getInstance(algorithm);
      sig.initVerify(publicKey);
      sig.update(data.getBytes());
      return sig.verify(BaseEncoding.base64().decode(sign));
    } catch (Exception e) {
      LOG.warn("", e);
      return false;
    }
  }

  public static void main(String[] args) {
    String text = "你是小铃铛 ExpTime=1226577284468$Pid=100013$Sid=rlpm001 你好啊!!!&&";
    System.out.println("text = " + text);
    RSAUtil rsa = create();
    KeyPair keyPair = rsa.generateKeyPair();
    String pubKey = rsa.getPublicKeyWithBase64(keyPair);
    String priKey = rsa.getPrivateKeyWithBase64(keyPair);
    String encrypted = rsa.encryptByPublicKey(pubKey, text);
    String decrypted = rsa.decryptByPrivateKey(priKey, encrypted);
    System.out.println(encrypted);
    System.out.println(decrypted);
    String sign = rsa.signSha256withRSA(priKey, text);
    System.out.println("sign = " + sign);
    boolean verify = rsa.verifySHA256WithRSA(pubKey, text, sign);
    System.out.println("verify = " + verify);
  }
}
