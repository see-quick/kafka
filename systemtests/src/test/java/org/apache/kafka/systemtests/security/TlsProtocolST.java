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
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterSystemTemplate;
import org.apache.kafka.systemtests.utils.ClientUtils;
import org.apache.kafka.systemtests.utils.security.TlsCluster;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RH FIPS restricts TLS to
 * TLSv1.2/TLSv1.3, so a broker/client pinned to either protocol alone must still work.
 */
public class TlsProtocolST {

    private static final int NUM_MESSAGES = 100;
    private static final List<String> PROTOCOLS = List.of("TLSv1.2", "TLSv1.3");

    static List<ClusterConfig> generateProtocolConfigs() throws Exception {
        List<ClusterConfig> configs = new ArrayList<>();
        for (String protocol : PROTOCOLS) {
            configs.add(TlsCluster.builder()
                .env("KAFKA_SSL_ENABLED_PROTOCOLS", protocol)
                .env("KAFKA_SSL_PROTOCOL", protocol)
                .clientConfig(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, List.of(protocol))
                .clientConfig(SslConfigs.SSL_PROTOCOL_CONFIG, protocol)
                .tag("tlsProtocol=" + protocol)
                .build());
        }
        return configs;
    }

    @ClusterSystemTemplate("generateProtocolConfigs")
    void testProduceConsumePinnedToSingleTlsProtocol(ClusterInstance cluster) throws Exception {
        String topicName = "tls-protocol-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }
}
