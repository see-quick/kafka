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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ExecutionMode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Builds the {@link ClusterConfig} for a TLS-enabled container cluster: mints the certificates,
 * mounts the stores into the broker container, sets the matching {@code KAFKA_SSL_*} environment
 * and configures the test JVM's client to trust the same CA.
 *
 * <p>Exists because that wiring is identical across every TLS system test apart from two or three
 * knobs, and hand-rolling it per test made it easy to get subtly wrong: notably the SAN list (which
 * must cover every node id under both isolated and combined KRaft) and the fact that PEM stores
 * must not carry a password while PKCS12 stores must.
 *
 * <pre>{@code
 * static List<ClusterConfig> generatePemConfigs() throws Exception {
 *     return List.of(TlsCluster.builder().brokerStoreType(StoreType.PEM).build());
 * }
 * }</pre>
 */
public final class TlsCluster {

    /** Where the broker container's entrypoint expects mounted TLS material to live. */
    private static final String SECRETS_DIR = "/etc/kafka/secrets";

    private TlsCluster() {
    }

    /** The keystore/truststore encodings a broker or client can be pointed at. */
    public enum StoreType {
        PKCS12(TlsFixture.STORE_TYPE_PKCS12),
        PEM("PEM");

        private final String configValue;

        StoreType(String configValue) {
            this.configValue = configValue;
        }

        /** The value Kafka's {@code ssl.*.type} configs expect. */
        public String configValue() {
            return configValue;
        }

        private String fileExtension() {
            return "." + configValue.toLowerCase(Locale.ROOT);
        }
    }

    /** A builder backed by a freshly generated CA. */
    public static Builder builder() throws GeneralSecurityException {
        return new Builder(TlsFixture.generate());
    }

    /** A builder reusing an existing CA, for tests that need several clusters to trust each other. */
    public static Builder builder(TlsFixture tls) {
        return new Builder(tls);
    }

    public static final class Builder {
        private final TlsFixture tls;
        private int brokers = 1;
        private int controllers = 1;
        private StoreType brokerStoreType = StoreType.PKCS12;
        private StoreType clientTrustStoreType = StoreType.PEM;
        private SecurityProtocol securityProtocol = SecurityProtocol.SSL;
        private String saslMechanism;
        private String containerImage;
        private final Map<String, String> extraEnv = new HashMap<>();
        private final Map<String, Object> clientSslConfig = new HashMap<>();
        private final Map<String, Object> clientSaslConfig = new HashMap<>();
        private final List<String> tags = new ArrayList<>();

        private Builder(TlsFixture tls) {
            this.tls = tls;
        }

        public Builder brokers(int brokers) {
            this.brokers = brokers;
            return this;
        }

        public Builder controllers(int controllers) {
            this.controllers = controllers;
            return this;
        }

        /** How the broker's own keystore and truststore are encoded. Defaults to PKCS12. */
        public Builder brokerStoreType(StoreType storeType) {
            this.brokerStoreType = storeType;
            return this;
        }

        /**
         * How the test JVM's client truststore is encoded. Defaults to PEM, which needs no
         * temporary file. Worth setting to PKCS12 when the test is specifically about the broker's
         * PEM handling, so a failure cannot be blamed on the client's own PEM parsing.
         */
        public Builder clientTrustStoreType(StoreType storeType) {
            this.clientTrustStoreType = storeType;
            return this;
        }

        /** Defaults to {@link SecurityProtocol#SSL}. */
        public Builder securityProtocol(SecurityProtocol securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder saslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
            return this;
        }

        /** Pins the broker container image, e.g. a locally built FIPS-capable one. */
        public Builder containerImage(String containerImage) {
            this.containerImage = containerImage;
            return this;
        }

        /** An extra broker environment variable, e.g. {@code KAFKA_SSL_ENABLED_PROTOCOLS}. */
        public Builder env(String key, String value) {
            extraEnv.put(key, value);
            return this;
        }

        /** An extra client config, e.g. {@code ssl.protocol}. */
        public Builder clientConfig(String key, Object value) {
            clientSslConfig.put(key, value);
            return this;
        }

        public Builder clientSaslConfig(Map<String, Object> configs) {
            clientSaslConfig.putAll(configs);
            return this;
        }

        /** A tag distinguishing this configuration in the test's display name. */
        public Builder tag(String tag) {
            tags.add(tag);
            return this;
        }

        /** The CA behind this cluster, for tests that need to mint further certificates from it. */
        public TlsFixture tls() {
            return tls;
        }

        public ClusterConfig build() throws GeneralSecurityException {
            Map<String, byte[]> filesToMount = new HashMap<>();
            Map<String, String> env = new HashMap<>();
            env.put("KAFKA_SSL_CLIENT_AUTH", "none");
            configureBrokerStores(filesToMount, env);
            // Applied last so a test can override anything the defaults set.
            env.putAll(extraEnv);

            Map<String, Object> clientConfig = new HashMap<>();
            clientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
            configureClientTrust(clientConfig);
            clientConfig.putAll(clientSslConfig);

            ClusterConfig.Builder config = ClusterConfig.defaultBuilder()
                .setExecutionModes(Set.of(ExecutionMode.CONTAINER))
                .setBrokers(brokers)
                .setControllers(controllers)
                .setBrokerSecurityProtocol(securityProtocol)
                .setExtraEnv(env)
                .setFilesToMount(filesToMount)
                .setClientSslConfig(clientConfig);

            if (saslMechanism != null) {
                config.setSaslMechanism(saslMechanism);
            }
            if (!clientSaslConfig.isEmpty()) {
                config.setClientSaslConfig(clientSaslConfig);
            }
            if (containerImage != null) {
                config.setContainerImage(containerImage);
            }
            if (!tags.isEmpty()) {
                config.setTags(tags);
            }
            return config.build();
        }

        private void configureBrokerStores(Map<String, byte[]> filesToMount, Map<String, String> env) throws GeneralSecurityException {
            String keyStorePath = SECRETS_DIR + "/kafka.server.keystore" + brokerStoreType.fileExtension();
            String trustStorePath = SECRETS_DIR + "/kafka.server.truststore" + brokerStoreType.fileExtension();

            if (brokerStoreType == StoreType.PEM) {
                filesToMount.put(keyStorePath, tls.serverKeyStorePem(sanDnsNames()));
                filesToMount.put(trustStorePath, tls.caCertificatePem().getBytes(StandardCharsets.UTF_8));
            } else {
                filesToMount.put(keyStorePath, tls.serverKeyStoreBytes(brokerStoreType.configValue(), sanDnsNames()));
                filesToMount.put(trustStorePath, tls.trustStoreBytes(brokerStoreType.configValue()));
                // PEM carries no store password and DefaultSslEngineFactory rejects one if set,
                // so these are deliberately only set for the keystore-backed types.
                String password = new String(TlsFixture.STORE_PASSWORD);
                env.put("KAFKA_SSL_KEYSTORE_PASSWORD", password);
                env.put("KAFKA_SSL_TRUSTSTORE_PASSWORD", password);
            }

            env.put("KAFKA_SSL_KEYSTORE_LOCATION", keyStorePath);
            env.put("KAFKA_SSL_KEYSTORE_TYPE", brokerStoreType.configValue());
            env.put("KAFKA_SSL_TRUSTSTORE_LOCATION", trustStorePath);
            env.put("KAFKA_SSL_TRUSTSTORE_TYPE", brokerStoreType.configValue());
        }

        private void configureClientTrust(Map<String, Object> clientConfig) throws GeneralSecurityException {
            clientConfig.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, clientTrustStoreType.configValue());
            if (clientTrustStoreType == StoreType.PEM) {
                clientConfig.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, tls.caCertificatePem());
            } else {
                clientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                    writeTempStore(tls.trustStoreBytes(clientTrustStoreType.configValue())));
                clientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, new String(TlsFixture.STORE_PASSWORD));
            }
        }

        /**
         * Isolated KRaft assigns node ids {@code [0, controllers)} to controllers and the rest to
         * brokers, while combined mode reuses ids {@code [0, max(brokers, controllers))} for both
         * roles. Covering every id up to {@code brokers + controllers} means the broker's alias is
         * covered under either topology without having to know which id ends up being the broker.
         * {@code localhost} covers the test JVM reaching the broker through a mapped port.
         */
        private String[] sanDnsNames() {
            int totalNodeIds = brokers + controllers;
            return IntStream.rangeClosed(0, totalNodeIds)
                .mapToObj(i -> i == totalNodeIds ? "localhost" : "kafka-" + i)
                .toArray(String[]::new);
        }

        private static String writeTempStore(byte[] store) {
            try {
                Path path = Files.createTempFile("systemtests-client-truststore", ".p12");
                path.toFile().deleteOnExit();
                Files.write(path, store);
                return path.toString();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to write client truststore", e);
            }
        }
    }
}
