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

package org.apache.kafka.systemtests.security;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.JaasUtils;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ExecutionMode;
import org.apache.kafka.systemtests.utils.ClientUtils;
import org.apache.kafka.systemtests.utils.security.TlsFixture;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * SASL_SSL with SCRAM-SHA-512 using a
 * password well over the 16-byte floor a FIPS-restricted {@code Mac} provider enforces on the
 * HMAC key {@code ScramFormatter.hi()} derives it into (see {@link JaasUtils#KAFKA_SCRAM_ADMIN_PASSWORD}).
 * This is a positive-path test; the negative case (short password) only fails under real FIPS
 * and belongs in Tier 2 if pursued, not here.
 */
public class ScramSslST {

    private static final int NUM_MESSAGES = 100;

    static List<ClusterConfig> generateScramSslConfigs() throws Exception {
        TlsFixture tls = TlsFixture.generate();
        byte[] serverKeyStore = tls.serverKeyStoreBytes(TlsFixture.STORE_TYPE_PKCS12, "kafka-0", "kafka-1", "localhost");
        byte[] trustStore = tls.trustStoreBytes(TlsFixture.STORE_TYPE_PKCS12);
        String storePassword = new String(TlsFixture.STORE_PASSWORD);

        String keyStorePath = "/etc/kafka/secrets/kafka.server.keystore.p12";
        String trustStorePath = "/etc/kafka/secrets/kafka.server.truststore.p12";

        Map<String, byte[]> filesToMount = Map.of(
            keyStorePath, serverKeyStore,
            trustStorePath, trustStore);

        Map<String, String> extraEnv = new HashMap<>();
        extraEnv.put("KAFKA_SSL_KEYSTORE_LOCATION", keyStorePath);
        extraEnv.put("KAFKA_SSL_KEYSTORE_PASSWORD", storePassword);
        extraEnv.put("KAFKA_SSL_KEYSTORE_TYPE", TlsFixture.STORE_TYPE_PKCS12);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_LOCATION", trustStorePath);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_PASSWORD", storePassword);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_TYPE", TlsFixture.STORE_TYPE_PKCS12);
        extraEnv.put("KAFKA_SSL_CLIENT_AUTH", "none");

        Map<String, Object> clientSslConfig = Map.of(
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM",
            SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, tls.caCertificatePem());

        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required "
            + "username=\"" + JaasUtils.KAFKA_SCRAM_ADMIN + "\" "
            + "password=\"" + JaasUtils.KAFKA_SCRAM_ADMIN_PASSWORD + "\";";
        Map<String, Object> clientSaslConfig = Map.of(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name,
            SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512",
            SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        ClusterConfig config = ClusterConfig.defaultBuilder()
            .setExecutionModes(Set.of(ExecutionMode.CONTAINER))
            .setBrokers(1)
            .setControllers(1)
            .setBrokerSecurityProtocol(SecurityProtocol.SASL_SSL)
            .setSaslMechanism("SCRAM-SHA-512")
            .setExtraEnv(extraEnv)
            .setFilesToMount(filesToMount)
            .setClientSslConfig(clientSslConfig)
            .setClientSaslConfig(clientSaslConfig)
            .build();

        return List.of(config);
    }

    @Tag("system")
    @Timeout(120)
    @ClusterTemplate("generateScramSslConfigs")
    void testProduceConsumeAndAdminOverScramSsl(ClusterInstance cluster) throws Exception {
        String topicName = "scram-ssl-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        try (Admin admin = cluster.admin()) {
            assertFalse(admin.describeCluster().nodes().get().isEmpty(), "Should describe cluster over SASL_SSL");
        }

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }
}
