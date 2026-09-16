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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.JaasUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterSystemTest;
import org.apache.kafka.systemtests.utils.ClientUtils;

import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * SASL_SSL with SCRAM-SHA-512. The runtime creates the SCRAM user in the broker and authenticates
 * the client as it, using a password well over the 16-byte floor a FIPS-restricted {@code Mac}
 * provider enforces on the HMAC key {@code ScramFormatter.hi()} derives from it (see
 * {@link JaasUtils#KAFKA_SCRAM_ADMIN_PASSWORD}). Positive path only: the short-password rejection
 * exists only under real FIPS and would belong in {@link FipsST}.
 */
public class ScramSslST {

    private static final int NUM_MESSAGES = 100;

    @Timeout(120)
    @ClusterSystemTest(
        brokerSecurityProtocol = SecurityProtocol.SASL_SSL,
        serverProperties = @ClusterConfigProperty(key = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, value = "SCRAM-SHA-512")
    )
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
