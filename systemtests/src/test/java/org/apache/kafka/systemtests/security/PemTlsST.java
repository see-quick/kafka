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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The PEM keystore/truststore code path,
 * direct regression coverage for the KAFKA-20997 class of bug (PemStore eagerly loading a DSA
 * KeyFactory broke every PEM keystore, RSA-only ones included). PKCS#8, unencrypted, RSA.
 */
public class PemTlsST {

    private static final int NUM_MESSAGES = 100;

    static List<ClusterConfig> generatePemConfigs() throws Exception {
        TlsFixture tls = TlsFixture.generate();

        // Covers both isolated KRAFT (kafka-0=controller, kafka-1=broker) and combined CO_KRAFT
        // (kafka-0=both), since defaultBuilder() exercises both types and either id could end up
        // being the broker.
        byte[] serverPem = tls.serverKeyStorePem("kafka-0", "kafka-1", "localhost");
        String caPem = tls.caCertificatePem();

        String keyStorePath = "/etc/kafka/secrets/kafka.server.keystore.pem";
        String trustStorePath = "/etc/kafka/secrets/kafka.server.truststore.pem";

        Map<String, byte[]> filesToMount = Map.of(
            keyStorePath, serverPem,
            trustStorePath, caPem.getBytes(StandardCharsets.UTF_8));

        // PEM format carries no store password: DefaultSslEngineFactory rejects one if set.
        Map<String, String> extraEnv = new HashMap<>();
        extraEnv.put("KAFKA_SSL_KEYSTORE_LOCATION", keyStorePath);
        extraEnv.put("KAFKA_SSL_KEYSTORE_TYPE", "PEM");
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_LOCATION", trustStorePath);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_TYPE", "PEM");
        extraEnv.put("KAFKA_SSL_CLIENT_AUTH", "none");

        Map<String, Object> clientSslConfig = Map.of(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM",
            SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, caPem);

        ClusterConfig config = ClusterConfig.defaultBuilder()
            .setExecutionModes(Set.of(ExecutionMode.CONTAINER))
            .setBrokers(1)
            .setControllers(1)
            .setBrokerSecurityProtocol(SecurityProtocol.SSL)
            .setExtraEnv(extraEnv)
            .setFilesToMount(filesToMount)
            .setClientSslConfig(clientSslConfig)
            .build();

        return List.of(config);
    }

    @Tag("system")
    @Timeout(120)
    @ClusterTemplate("generatePemConfigs")
    void testProduceConsumeOverPemTls(ClusterInstance cluster) throws Exception {
        String topicName = "pem-tls-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }
}
