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
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
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
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Baseline TLS coverage for the container system test framework: SSL-only listener, PKCS12
 * broker keystore/truststore mounted into {@code /etc/kafka/secrets}, client trust configured
 * from an in-memory PEM export of the same CA. Exercises admin, produce and consume over TLS.
 *
 * <p>Uses {@code @ClusterTemplate} rather than {@code @ClusterSystemTest} because the TLS
 * material (a freshly generated CA and keystore) is only known at test-run time, not at
 * annotation-authoring time.
 */
public class SslST {

    private static final int NUM_MESSAGES = 100;
    private static final int NUM_BROKERS = 1;
    private static final int NUM_CONTROLLERS = 1;

    static List<ClusterConfig> generateSslConfigs() throws Exception {
        TlsFixture tls = TlsFixture.generate();

        // Isolated KRaft assigns node ids [0, controllers) to controllers and the rest to
        // brokers, while combined mode reuses ids [0, max(brokers, controllers)) for both roles.
        // Covering every id up to brokers + controllers means the broker's alias is covered
        // under either topology without needing to know in advance which id is the broker.
        int totalNodeIds = NUM_BROKERS + NUM_CONTROLLERS;
        String[] sanDnsNames = IntStream.rangeClosed(0, totalNodeIds)
            .mapToObj(i -> i == totalNodeIds ? "localhost" : "kafka-" + i)
            .toArray(String[]::new);

        String storeType = TlsFixture.STORE_TYPE_PKCS12;
        String storePassword = new String(TlsFixture.STORE_PASSWORD);
        byte[] serverKeyStore = tls.serverKeyStoreBytes(storeType, sanDnsNames);
        byte[] trustStore = tls.trustStoreBytes(storeType);

        String keyStorePath = "/etc/kafka/secrets/kafka.server.keystore.p12";
        String trustStorePath = "/etc/kafka/secrets/kafka.server.truststore.p12";

        Map<String, byte[]> filesToMount = Map.of(
            keyStorePath, serverKeyStore,
            trustStorePath, trustStore);

        Map<String, String> extraEnv = new HashMap<>();
        extraEnv.put("KAFKA_SSL_KEYSTORE_LOCATION", keyStorePath);
        extraEnv.put("KAFKA_SSL_KEYSTORE_PASSWORD", storePassword);
        extraEnv.put("KAFKA_SSL_KEYSTORE_TYPE", storeType);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_LOCATION", trustStorePath);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_PASSWORD", storePassword);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_TYPE", storeType);
        extraEnv.put("KAFKA_SSL_CLIENT_AUTH", "none");

        Map<String, Object> clientSslConfig = Map.of(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM",
            SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, tls.caCertificatePem());

        ClusterConfig config = ClusterConfig.defaultBuilder()
            .setExecutionModes(Set.of(ExecutionMode.CONTAINER))
            .setBrokers(NUM_BROKERS)
            .setControllers(NUM_CONTROLLERS)
            .setBrokerSecurityProtocol(SecurityProtocol.SSL)
            .setExtraEnv(extraEnv)
            .setFilesToMount(filesToMount)
            .setClientSslConfig(clientSslConfig)
            .build();

        return List.of(config);
    }

    @Tag("system")
    @Timeout(120)
    @ClusterTemplate("generateSslConfigs")
    void testProduceConsumeAndAdminOverSsl(ClusterInstance cluster) throws Exception {
        String topicName = "ssl-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        try (Admin admin = cluster.admin()) {
            assertFalse(admin.describeCluster().nodes().get().isEmpty(), "Should describe cluster over SSL");
        }

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }
}
