package net.xdob.ratly.grpc;

import net.xdob.ratly.security.TlsConf;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Ratly GRPC TLS configurations.
 */
public class GrpcTlsConfig extends TlsConf {
  private final boolean fileBasedConfig;

  public boolean isFileBasedConfig() {
    return fileBasedConfig;
  }

  public PrivateKey getPrivateKey() {
    return Optional.ofNullable(getKeyManager())
        .map(KeyManagerConf::getPrivateKey)
        .map(PrivateKeyConf::get)
        .orElse(null);
  }

  public File getPrivateKeyFile() {
    return Optional.ofNullable(getKeyManager())
        .map(KeyManagerConf::getPrivateKey)
        .map(PrivateKeyConf::getFile)
        .orElse(null);
  }

  public X509Certificate getCertChain() {
    return Optional.ofNullable(getKeyManager())
        .map(KeyManagerConf::getKeyCertificates)
        .map(CertificatesConf::get)
        .map(Iterable::iterator)
        .map(Iterator::next)
        .orElse(null);
  }

  public File getCertChainFile() {
    return Optional.ofNullable(getKeyManager())
        .map(KeyManagerConf::getKeyCertificates)
        .map(CertificatesConf::getFile)
        .orElse(null);
  }

  public List<X509Certificate> getTrustStore() {
    return (List<X509Certificate>) Optional.ofNullable(getTrustManager())
        .map(TrustManagerConf::getTrustCertificates)
        .map(CertificatesConf::get)
        .orElse(null);
  }

  public File getTrustStoreFile() {
    return Optional.ofNullable(getTrustManager())
        .map(TrustManagerConf::getTrustCertificates)
        .map(CertificatesConf::getFile)
        .orElse(null);
  }

  public boolean getMtlsEnabled() {
    return isMutualTls();
  }

  public GrpcTlsConfig(PrivateKey privateKey, X509Certificate certChain,
      List<X509Certificate> trustStore, boolean mTlsEnabled) {
    this(newBuilder(privateKey, certChain, trustStore, mTlsEnabled), false);
  }

  public GrpcTlsConfig(PrivateKey privateKey, X509Certificate certChain,
      X509Certificate trustStore, boolean mTlsEnabled) {
    this(privateKey, certChain, Collections.singletonList(trustStore), mTlsEnabled);
  }

  public GrpcTlsConfig(File privateKeyFile, File certChainFile,
      File trustStoreFile, boolean mTlsEnabled) {
    this(newBuilder(privateKeyFile, certChainFile, trustStoreFile, mTlsEnabled), true);
  }

  private GrpcTlsConfig(Builder builder, boolean fileBasedConfig) {
    super(builder);
    this.fileBasedConfig = fileBasedConfig;
  }

  public GrpcTlsConfig(KeyManager keyManager, TrustManager trustManager, boolean mTlsEnabled) {
    this(newBuilder(keyManager, trustManager, mTlsEnabled), false);
  }

  private static Builder newBuilder(PrivateKey privateKey, X509Certificate certChain,
      List<X509Certificate> trustStore, boolean mTlsEnabled) {
    final Builder b = newBuilder().setMutualTls(mTlsEnabled);
    Optional.ofNullable(trustStore).map(CertificatesConf::new).ifPresent(b::setTrustCertificates);
    Optional.ofNullable(privateKey).map(PrivateKeyConf::new).ifPresent(b::setPrivateKey);
    Optional.ofNullable(certChain).map(CertificatesConf::new).ifPresent(b::setKeyCertificates);
    return b;
  }

  private static Builder newBuilder(File privateKeyFile, File certChainFile, File trustStoreFile, boolean mTlsEnabled) {
    final Builder b = newBuilder().setMutualTls(mTlsEnabled);
    Optional.ofNullable(trustStoreFile).map(CertificatesConf::new).ifPresent(b::setTrustCertificates);
    Optional.ofNullable(privateKeyFile).map(PrivateKeyConf::new).ifPresent(b::setPrivateKey);
    Optional.ofNullable(certChainFile).map(CertificatesConf::new).ifPresent(b::setKeyCertificates);
    return b;
  }

  private static Builder newBuilder(KeyManager keyManager, TrustManager trustManager, boolean mTlsEnabled) {
    return newBuilder().setMutualTls(mTlsEnabled).setKeyManager(keyManager).setTrustManager(trustManager);
  }
}