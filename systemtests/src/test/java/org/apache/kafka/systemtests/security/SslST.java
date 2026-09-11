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
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterSystemTemplate;
import org.apache.kafka.systemtests.utils.ClientUtils;
import org.apache.kafka.systemtests.utils.security.TlsCluster;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Baseline TLS coverage for the container system test framework: SSL-only listener, PKCS12
 * broker keystore/truststore mounted into {@code /etc/kafka/secrets}, client trust configured
 * from an in-memory PEM export of the same CA. Exercises admin, produce and consume over TLS.
 *
 * <p>Uses {@code @ClusterSystemTemplate} rather than {@code @ClusterSystemTest} because the TLS
 * material (a freshly generated CA and keystore) is only known at test-run time, not at
 * annotation-authoring time.
 */
public class SslST {

    private static final int NUM_MESSAGES = 100;

    static List<ClusterConfig> generateSslConfigs() throws Exception {
        return List.of(TlsCluster.builder().build());
    }

    @ClusterSystemTemplate("generateSslConfigs")
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
