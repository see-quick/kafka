/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.systemtests.utils.security;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.test.TestSslUtils;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Locale;

/**
 * Generates an in-memory test CA and per-node/per-client certificates for container-based
 * system tests, so keystores/truststores can be mounted into broker containers without ever
 * touching disk on the test JVM side.
 *
 * <p>Deliberately builds keystores with {@link KeyStore#getInstance(String)} directly (default
 * {@value #STORE_TYPE_PKCS12}) rather than reusing {@code TestSslUtils.createKeyStore}/{@code
 * createTrustStore}, which are hardcoded to {@code JKS}.
 */
public final class TlsFixture {

    public static final String STORE_TYPE_PKCS12 = "PKCS12";
    public static final char[] STORE_PASSWORD = "systemtest-secret".toCharArray();
    private static final String CA_ALIAS = "ca";
    private static final String CERT_ALIAS = "cert";
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final int VALID_DAYS = 3650;

    private final String caDn;
    private final KeyPair caKeyPair;
    private final X509Certificate caCert;

    private TlsFixture(String caDn, KeyPair caKeyPair, X509Certificate caCert) {
        this.caDn = caDn;
        this.caKeyPair = caKeyPair;
        this.caCert = caCert;
    }

    /**
     * Generates a fresh, self-signed test CA. Each call produces an independent CA so tests
     * don't share trust roots unless they explicitly share a {@link TlsFixture} instance.
     */
    public static TlsFixture generate() throws GeneralSecurityException {
        try {
            String caDn = "CN=systemtests-ca";
            KeyPair caKeyPair = TestSslUtils.generateKeyPair("RSA");
            X509Certificate caCert = new TestSslUtils.CertificateBuilder(VALID_DAYS, SIGNATURE_ALGORITHM)
                .generateSignedCertificate(caDn, caKeyPair, 1, VALID_DAYS, null, null, true, false, false);
            return new TlsFixture(caDn, caKeyPair, caCert);
        } catch (GeneralSecurityException e) {
            throw e;
        } catch (Exception e) {
            throw new GeneralSecurityException("Failed to generate test CA", e);
        }
    }

    public X509Certificate caCertificate() {
        return caCert;
    }

    /** A PKCS12/JKS-encoded truststore (per {@code storeType}) containing only the CA certificate. */
    public byte[] trustStoreBytes(String storeType) throws GeneralSecurityException {
        try {
            KeyStore ts = KeyStore.getInstance(storeType);
            ts.load(null, null);
            ts.setCertificateEntry(CA_ALIAS, caCert);
            return encode(ts);
        } catch (GeneralSecurityException e) {
            throw e;
        } catch (Exception e) {
            throw new GeneralSecurityException("Failed to build trust store", e);
        }
    }

    /**
     * A keystore for a broker node, signed by this fixture's CA. The certificate's subject
     * alternative names cover both the container's Docker-network DNS alias ({@code kafka-<nodeId>})
     * and the loopback address the test JVM uses when it reaches the broker through a mapped port
     * ({@code localhost} / {@code 127.0.0.1}).
     */
    public byte[] nodeKeyStoreBytes(String storeType, int nodeId) throws GeneralSecurityException {
        return keyStoreBytes(storeType, SIGNATURE_ALGORITHM, "CN=kafka-" + nodeId,
            new String[] {"kafka-" + nodeId, "localhost"}, true);
    }

    /**
     * A single keystore shared by every node in a cluster, whose SAN list covers every node's
     * DNS alias. Simpler than minting one certificate per node when the topology already fixes
     * the small, known set of aliases (e.g. {@code kafka-0}, {@code kafka-1}, ...) up front.
     */
    public byte[] serverKeyStoreBytes(String storeType, String... sanDnsNames) throws GeneralSecurityException {
        return keyStoreBytes(storeType, SIGNATURE_ALGORITHM, "CN=kafka-broker", sanDnsNames, true);
    }

    /**
     * Like {@link #serverKeyStoreBytes(String, String...)}, but signed with an explicit algorithm
     * rather than this fixture's default. Exists for negative tests, e.g. asserting a
     * {@code SHA1withRSA}-signed certificate is rejected under a modern {@code jdk.tls.disabledAlgorithms}.
     */
    public byte[] serverKeyStoreBytesWithSignature(String storeType, String signatureAlgorithm, String... sanDnsNames)
            throws GeneralSecurityException {
        return keyStoreBytes(storeType, signatureAlgorithm, "CN=kafka-broker", sanDnsNames, true);
    }

    /**
     * The broker keystore as a single PEM file: the certificate chain (leaf, then CA) followed by
     * an unencrypted PKCS#8 private key, in the concatenated layout
     * {@code DefaultSslEngineFactory.FileBasedPemStore} expects when {@code ssl.keystore.type=PEM}
     * and {@code ssl.keystore.location} both point at it.
     */
    public byte[] serverKeyStorePem(String... sanDnsNames) throws GeneralSecurityException {
        return pemKeyAndCertificateChain(SIGNATURE_ALGORITHM, "CN=kafka-broker", sanDnsNames, true);
    }

    /** A client keystore for mTLS tests, signed by this fixture's CA. */
    public byte[] clientKeyStoreBytes(String storeType, String commonName) throws GeneralSecurityException {
        return keyStoreBytes(storeType, SIGNATURE_ALGORITHM, "CN=" + commonName, new String[] {"localhost"}, false);
    }

    /** The CA certificate PEM-encoded, for use with {@code ssl.truststore.type=PEM} on the client side. */
    public String caCertificatePem() throws GeneralSecurityException {
        try {
            Path tempTrustStore = Files.createTempFile("systemtests-ca-truststore", "." + STORE_TYPE_PKCS12.toLowerCase(Locale.ROOT));
            try {
                Files.write(tempTrustStore, trustStoreBytes(STORE_TYPE_PKCS12));
                Password password = new Password(new String(STORE_PASSWORD));
                return TestSslUtils.exportCertificates(tempTrustStore.toString(), password, STORE_TYPE_PKCS12).value();
            } finally {
                Files.deleteIfExists(tempTrustStore);
            }
        } catch (GeneralSecurityException e) {
            throw e;
        } catch (Exception e) {
            throw new GeneralSecurityException("Failed to export CA certificate as PEM", e);
        }
    }

    private byte[] keyStoreBytes(String storeType, String signatureAlgorithm, String dn, String[] sanDnsNames, boolean isServerCert)
            throws GeneralSecurityException {
        try {
            KeyPair keyPair = TestSslUtils.generateKeyPair("RSA");
            X509Certificate cert = new TestSslUtils.CertificateBuilder(VALID_DAYS, signatureAlgorithm)
                .sanNames(sanDnsNames, new InetAddress[] {loopbackAddress()})
                .generateSignedCertificate(dn, keyPair, 1, VALID_DAYS, caDn, caKeyPair, false, isServerCert, true);
            KeyStore ks = KeyStore.getInstance(storeType);
            ks.load(null, null);
            ks.setKeyEntry(CERT_ALIAS, keyPair.getPrivate(), STORE_PASSWORD, new Certificate[] {cert, caCert});
            return encode(ks);
        } catch (GeneralSecurityException e) {
            throw e;
        } catch (Exception e) {
            throw new GeneralSecurityException("Failed to build key store for dn=" + dn, e);
        }
    }

    /**
     * Builds a PKCS12 keystore purely as scratch to hand to {@code TestSslUtils.exportCertificates}/
     * {@code exportPrivateKey} (which only accept a keystore file path), then discards it. Returns
     * the PEM-encoded certificate chain immediately followed by the unencrypted PKCS#8 private key.
     */
    private byte[] pemKeyAndCertificateChain(String signatureAlgorithm, String dn, String[] sanDnsNames, boolean isServerCert)
            throws GeneralSecurityException {
        try {
            byte[] scratchKeyStore = keyStoreBytes(STORE_TYPE_PKCS12, signatureAlgorithm, dn, sanDnsNames, isServerCert);
            Path tempKeyStore = Files.createTempFile("systemtests-node-keystore", "." + STORE_TYPE_PKCS12.toLowerCase(Locale.ROOT));
            try {
                Files.write(tempKeyStore, scratchKeyStore);
                Password password = new Password(new String(STORE_PASSWORD));
                String certChain = TestSslUtils.exportCertificates(tempKeyStore.toString(), password, STORE_TYPE_PKCS12).value();
                String key = TestSslUtils.exportPrivateKey(tempKeyStore.toString(), password, password, STORE_TYPE_PKCS12, null).value();
                return (certChain + key).getBytes(StandardCharsets.UTF_8);
            } finally {
                Files.deleteIfExists(tempKeyStore);
            }
        } catch (GeneralSecurityException e) {
            throw e;
        } catch (Exception e) {
            throw new GeneralSecurityException("Failed to export PEM key/cert for dn=" + dn, e);
        }
    }

    private static byte[] encode(KeyStore ks) throws GeneralSecurityException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ks.store(out, STORE_PASSWORD);
            return out.toByteArray();
        } catch (Exception e) {
            throw new GeneralSecurityException("Failed to encode key store", e);
        }
    }

    private static InetAddress loopbackAddress() {
        try {
            return InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }
}
