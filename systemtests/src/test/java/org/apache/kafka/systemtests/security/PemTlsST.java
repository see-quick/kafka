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
import org.apache.kafka.systemtests.utils.ClientUtils;

import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The PEM keystore/truststore code path on the broker (PKCS#8, unencrypted, RSA): direct
 * regression coverage for the KAFKA-20997 class of bug, where {@code PemStore} eagerly loading a
 * DSA {@code KeyFactory} broke every PEM keystore, RSA-only ones included. The client trusts the
 * broker through a PKCS12 store, so a failure here is the broker's PEM handling and nothing else.
 *
 * <p>On a stock JVM this only proves PEM stores work; KAFKA-20997 itself needs a FIPS-restricted
 * provider list to fire, which is what {@link FipsST} covers.
 */
public class PemTlsST {

    private static final int NUM_MESSAGES = 100;

    @Timeout(120)
    @ClusterSystemTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        serverProperties = {
            @ClusterConfigProperty(key = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, value = "PEM"),
            @ClusterConfigProperty(key = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, value = "PEM")
        }
    )
    void testProduceConsumeOverPemTls(ClusterInstance cluster) throws Exception {
        String topicName = "pem-tls-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }
}
