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
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterSystemTemplate;
import org.apache.kafka.systemtests.utils.ClientUtils;
import org.apache.kafka.systemtests.utils.security.TlsCluster;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The PEM keystore/truststore code path,
 * direct regression coverage for the KAFKA-20997 class of bug (PemStore eagerly loading a DSA
 * KeyFactory broke every PEM keystore, RSA-only ones included). PKCS#8, unencrypted, RSA.
 *
 * <p>Note this only reproduces KAFKA-20997 itself on a FIPS-restricted JVM, which the default
 * container image is not; see {@code FipsSt.testProduceConsumeOverPemTlsUnderFips} for that.
 */
public class PemTlsST {

    private static final int NUM_MESSAGES = 100;

    static List<ClusterConfig> generatePemConfigs() throws Exception {
        return List.of(TlsCluster.builder()
            .brokerStoreType(TlsCluster.StoreType.PEM)
            .build());
    }

    @ClusterSystemTemplate("generatePemConfigs")
    void testProduceConsumeOverPemTls(ClusterInstance cluster) throws Exception {
        String topicName = "pem-tls-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }
}
