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

package org.apache.kafka.common.test.container;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * TLS material for a {@link KafkaContainerCluster}, the container-side counterpart of
 * {@link org.apache.kafka.common.test.SslManager}. Mints a CA and one server certificate whose
 * subject alternative names cover every node's Docker-network alias plus {@code localhost}, encodes
 * them as the keystore and truststore types the broker is configured with, and hands out the files
 * to copy into the containers, the {@code KAFKA_SSL_*} environment pointing the broker at them, and
 * the client configuration that trusts the same CA.
 *
 * <p>This is what lets a test declare {@code @ClusterSystemTest(brokerSecurityProtocol = SSL)} and
 * nothing else, the way the in-memory {@code @ClusterTest} already can: the material only exists at
 * run time, so it is the runtime's job to make it, not the test's.
 *
 * <p>Stores are built with {@link KeyStore#getInstance(String)} directly rather than through
 * {@code TestSslUtils.createKeyStore}, which is hardcoded to JKS. The client side always gets a
 * PKCS12 truststore, whatever the broker uses, so a failure in a PEM-configured broker cannot be
 * blamed on the test JVM's own PEM parsing.
 */
public final class ContainerSslManager implements Closeable {

    /** Where the image's entrypoint expects mounted TLS material; writable by its non-root user. */
    public static final String SECRETS_DIR = "/etc/kafka/secrets";

    /** Used for whichever of the broker's stores the test does not configure a type for. */
    public static final String DEFAULT_STORE_TYPE = "PKCS12";

    private static final String STORE_PASSWORD = "container-test-secret";
    private static final String KEY_ALGORITHM = "RSA";
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final int VALID_DAYS = 365;
    private static final String CA_DN = "CN=kafka-container-ca";
    private static final String SERVER_DN = "CN=kafka-broker";
    private static final String CA_ALIAS = "ca";
    private static final String SERVER_ALIAS = "server";

    private final String keyStoreType;
    private final String trustStoreType;
    private final X509Certificate caCert;
    private final KeyStore serverKeyStore;
    private final File clientTrustStoreFile;

    /**
     * @param keyStoreType   the broker's {@code ssl.keystore.type}: a {@link KeyStore} type or {@code PEM}
     * @param trustStoreType the broker's {@code ssl.truststore.type}: a {@link KeyStore} type or {@code PEM}
     * @param sanDnsNames    every DNS name the server certificate must be valid for
     */
    public ContainerSslManager(String keyStoreType, String trustStoreType, String... sanDnsNames) {
        this.keyStoreType = keyStoreType;
        this.trustStoreType = trustStoreType;
        try {
            KeyPair caKeyPair = TestSslUtils.generateKeyPair(KEY_ALGORITHM);
            this.caCert = new TestSslUtils.CertificateBuilder(VALID_DAYS, SIGNATURE_ALGORITHM)
                .generateSignedCertificate(CA_DN, caKeyPair, 1, VALID_DAYS, null, null, true, false, false);

            KeyPair serverKeyPair = TestSslUtils.generateKeyPair(KEY_ALGORITHM);
            X509Certificate serverCert = new TestSslUtils.CertificateBuilder(VALID_DAYS, SIGNATURE_ALGORITHM)
                .sanNames(sanDnsNames, new InetAddress[] {InetAddress.getByName("127.0.0.1")})
                .generateSignedCertificate(SERVER_DN, serverKeyPair, 1, VALID_DAYS, CA_DN, caKeyPair, false, true, false);

            // Kept as a PKCS12 in-memory store and re-encoded on demand: PEM export goes through
            // TestSslUtils, which only reads from a keystore file.
            this.serverKeyStore = KeyStore.getInstance(DEFAULT_STORE_TYPE);
            serverKeyStore.load(null, null);
            serverKeyStore.setKeyEntry(SERVER_ALIAS, serverKeyPair.getPrivate(), STORE_PASSWORD.toCharArray(),
                new Certificate[] {serverCert, caCert});

            this.clientTrustStoreFile = org.apache.kafka.test.TestUtils.tempFile("kafka.client.truststore", ".p12");
            Files.write(clientTrustStoreFile.toPath(), trustStoreBytes(DEFAULT_STORE_TYPE));
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate TLS material for the container cluster", e);
        }
    }

    /**
     * Every node id under both KRaft topologies, so the certificate is valid for whichever ids end
     * up being brokers: isolated mode gives controllers {@code [0, controllers)} and brokers the
     * rest, while combined mode reuses {@code [0, max(brokers, controllers))} for both roles.
     * {@code localhost} is how the test JVM reaches a broker through its mapped port.
     */
    public static String[] sanDnsNames(int brokers, int controllers) {
        int totalNodeIds = brokers + controllers;
        return IntStream.rangeClosed(0, totalNodeIds)
            .mapToObj(i -> i == totalNodeIds ? "localhost" : KafkaContainerCluster.DNS_PREFIX + i)
            .toArray(String[]::new);
    }

    /** The broker's keystore and truststore, keyed by the container path each is copied to. */
    public Map<String, byte[]> filesToMount() {
        try {
            Map<String, byte[]> files = new HashMap<>();
            files.put(keyStorePath(), isPem(keyStoreType) ? serverKeyStorePem() : serverKeyStoreBytes(keyStoreType));
            files.put(trustStorePath(), isPem(trustStoreType) ? caCertificatePem() : trustStoreBytes(trustStoreType));
            return files;
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode TLS stores for the container cluster", e);
        }
    }

    /**
     * The {@code KAFKA_SSL_*} environment pointing the broker at {@link #filesToMount()}. Store
     * passwords are only set for keystore-backed types, since {@link DefaultSslEngineFactory}
     * rejects a password on a PEM store.
     */
    public Map<String, String> brokerEnv() {
        Map<String, String> env = new HashMap<>();
        env.put(KafkaEnvVars.SSL_CLIENT_AUTH, "none");
        env.put(KafkaEnvVars.SSL_KEYSTORE_LOCATION, keyStorePath());
        env.put(KafkaEnvVars.SSL_KEYSTORE_TYPE, keyStoreType);
        env.put(KafkaEnvVars.SSL_TRUSTSTORE_LOCATION, trustStorePath());
        env.put(KafkaEnvVars.SSL_TRUSTSTORE_TYPE, trustStoreType);
        if (!isPem(keyStoreType)) {
            env.put(KafkaEnvVars.SSL_KEYSTORE_PASSWORD, STORE_PASSWORD);
        }
        if (!isPem(trustStoreType)) {
            env.put(KafkaEnvVars.SSL_TRUSTSTORE_PASSWORD, STORE_PASSWORD);
        }
        return env;
    }

    /** Client-side {@code ssl.*} configuration trusting this manager's CA. */
    public Map<String, Object> clientSslConfig() {
        return Map.of(
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, DEFAULT_STORE_TYPE,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStoreFile.getPath(),
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, STORE_PASSWORD);
    }

    @Override
    public void close() throws IOException {
        Utils.delete(clientTrustStoreFile);
    }

    private static boolean isPem(String storeType) {
        return DefaultSslEngineFactory.PEM_TYPE.equalsIgnoreCase(storeType);
    }

    private String keyStorePath() {
        return SECRETS_DIR + "/kafka.server.keystore." + keyStoreType.toLowerCase(Locale.ROOT);
    }

    private String trustStorePath() {
        return SECRETS_DIR + "/kafka.server.truststore." + trustStoreType.toLowerCase(Locale.ROOT);
    }

    private byte[] trustStoreBytes(String storeType) throws Exception {
        KeyStore ts = KeyStore.getInstance(storeType);
        ts.load(null, null);
        ts.setCertificateEntry(CA_ALIAS, caCert);
        return encode(ts);
    }

    private byte[] serverKeyStoreBytes(String storeType) throws Exception {
        KeyStore ks = KeyStore.getInstance(storeType);
        ks.load(null, null);
        ks.setKeyEntry(SERVER_ALIAS, serverKeyStore.getKey(SERVER_ALIAS, STORE_PASSWORD.toCharArray()),
            STORE_PASSWORD.toCharArray(), serverKeyStore.getCertificateChain(SERVER_ALIAS));
        return encode(ks);
    }

    /**
     * The certificate chain (leaf, then CA) followed by the unencrypted PKCS#8 private key, in the
     * single-file layout {@code DefaultSslEngineFactory.FileBasedPemStore} expects when
     * {@code ssl.keystore.type=PEM} and {@code ssl.keystore.location} both point at it.
     */
    private byte[] serverKeyStorePem() throws Exception {
        File scratch = org.apache.kafka.test.TestUtils.tempFile("kafka.server.keystore", ".p12");
        try {
            Files.write(scratch.toPath(), encode(serverKeyStore));
            Password password = new Password(STORE_PASSWORD);
            String chain = TestSslUtils.exportCertificates(scratch.getPath(), password, DEFAULT_STORE_TYPE).value();
            String key = TestSslUtils.exportPrivateKey(scratch.getPath(), password, password, DEFAULT_STORE_TYPE, null).value();
            return (chain + key).getBytes(StandardCharsets.UTF_8);
        } finally {
            Utils.delete(scratch);
        }
    }

    private byte[] caCertificatePem() throws Exception {
        File scratch = org.apache.kafka.test.TestUtils.tempFile("kafka.server.truststore", ".p12");
        try {
            Files.write(scratch.toPath(), trustStoreBytes(DEFAULT_STORE_TYPE));
            return TestSslUtils.exportCertificates(scratch.getPath(), new Password(STORE_PASSWORD), DEFAULT_STORE_TYPE)
                .value().getBytes(StandardCharsets.UTF_8);
        } finally {
            Utils.delete(scratch);
        }
    }

    private static byte[] encode(KeyStore store) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        store.store(out, STORE_PASSWORD.toCharArray());
        return out.toByteArray();
    }
}
