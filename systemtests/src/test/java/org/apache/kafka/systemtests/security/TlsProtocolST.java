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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterSystemTest;
import org.apache.kafka.common.test.api.ClusterSystemTests;
import org.apache.kafka.systemtests.utils.ClientUtils;

import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A broker and a client each pinned to a single TLS protocol version still talk to each other.
 * FIPS crypto policies restrict TLS to TLSv1.2 and TLSv1.3, so both have to work on their own.
 */
public class TlsProtocolST {

    private static final int NUM_MESSAGES = 100;

    @Timeout(120)
    @ClusterSystemTests({
        @ClusterSystemTest(
            brokerSecurityProtocol = SecurityProtocol.SSL,
            serverProperties = {
                @ClusterConfigProperty(key = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, value = "TLSv1.2"),
                @ClusterConfigProperty(key = SslConfigs.SSL_PROTOCOL_CONFIG, value = "TLSv1.2")
            },
            tags = "tlsProtocol=TLSv1.2"
        ),
        @ClusterSystemTest(
            brokerSecurityProtocol = SecurityProtocol.SSL,
            serverProperties = {
                @ClusterConfigProperty(key = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, value = "TLSv1.3"),
                @ClusterConfigProperty(key = SslConfigs.SSL_PROTOCOL_CONFIG, value = "TLSv1.3")
            },
            tags = "tlsProtocol=TLSv1.3"
        )
    })
    void testProduceConsumePinnedToSingleTlsProtocol(ClusterInstance cluster) throws Exception {
        String protocol = cluster.config().serverProperties().get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        // Pin the client to the same single version as the broker, so neither side can fall back.
        Map<String, String> clientConfig = Map.of(
            SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, protocol,
            SslConfigs.SSL_PROTOCOL_CONFIG, protocol);

        String topicName = "tls-protocol-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        ClientUtils.produceMessages(cluster, topicName, 0, NUM_MESSAGES, clientConfig);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L, clientConfig);

        assertEquals(NUM_MESSAGES, records.size(), "Expected " + NUM_MESSAGES + " messages over " + protocol);
    }
}
