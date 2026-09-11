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
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.JaasUtils;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterSystemTemplate;
import org.apache.kafka.systemtests.utils.ClientUtils;
import org.apache.kafka.systemtests.utils.security.TlsCluster;

import java.util.List;
import java.util.Map;

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
    private static final String MECHANISM = "SCRAM-SHA-512";

    static List<ClusterConfig> generateScramSslConfigs() throws Exception {
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required "
            + "username=\"" + JaasUtils.KAFKA_SCRAM_ADMIN + "\" "
            + "password=\"" + JaasUtils.KAFKA_SCRAM_ADMIN_PASSWORD + "\";";

        return List.of(TlsCluster.builder()
            .securityProtocol(SecurityProtocol.SASL_SSL)
            .saslMechanism(MECHANISM)
            .clientSaslConfig(Map.of(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name,
                SaslConfigs.SASL_MECHANISM, MECHANISM,
                SaslConfigs.SASL_JAAS_CONFIG, jaasConfig))
            .build());
    }

    @ClusterSystemTemplate("generateScramSslConfigs")
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
