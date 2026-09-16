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
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterSystemTest;
import org.apache.kafka.systemtests.utils.ClientUtils;
import org.apache.kafka.systemtests.utils.security.FipsEnabled;
import org.apache.kafka.systemtests.utils.security.FipsUtils;

import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * TLS on a broker whose JDK runs with FIPS-restricted providers for real: the image selected with
 * {@code -PkafkaSystemTestsImage} (a UBI one, say) on a host whose kernel is in FIPS mode, where
 * the container inherits {@code /proc/sys/crypto/fips_enabled=1} and Red Hat's provider swap
 * activates. {@link FipsEnabled} keeps these out of every other run: the {@code fips} tag is
 * excluded unless {@code -PkafkaSystemtestsFips=true}, and the tests skip on any non-FIPS host.
 *
 * <p>Each test first checks the broker container itself reports a FIPS kernel, so a pass cannot
 * come from a broker that happened to run somewhere else.
 */
@FipsEnabled
public class FipsST {

    private static final int NUM_MESSAGES = 100;

    /**
     * PKCS12 stores stay available under Red Hat's FIPS providers, so this is the baseline that the
     * common TLS path still works. It does <em>not</em> cover KAFKA-20997, which only fires for PEM
     * stores; that is {@link #testProduceConsumeOverPemTlsUnderFips(ClusterInstance)}.
     */
    @Timeout(180)
    @ClusterSystemTest(brokerSecurityProtocol = SecurityProtocol.SSL)
    void testProduceConsumeAndAdminOverTlsUnderFips(ClusterInstance cluster) throws Exception {
        FipsUtils.assertBrokerInFipsMode(cluster);

        String topicName = "fips-tls-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        try (Admin admin = cluster.admin()) {
            assertFalse(admin.describeCluster().nodes().get().isEmpty(), "Should describe cluster over TLS under FIPS");
        }

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }

    /**
     * The test that would have caught KAFKA-20997 at the system level. {@code PemStore}'s static
     * initializer eagerly builds an RSA, a DSA and an EC {@code KeyFactory}, and DSA is unavailable
     * under Red Hat's FIPS providers, so without the fix the broker dies building its SSL channel
     * and this fails at cluster startup rather than at produce/consume. A PKCS12 broker never loads
     * the class, which is why the test above passes with or without the fix. The client side
     * trusts the broker through a PKCS12 store, so the PEM path under test is the broker's alone.
     */
    @Timeout(180)
    @ClusterSystemTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        serverProperties = {
            @ClusterConfigProperty(key = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, value = "PEM"),
            @ClusterConfigProperty(key = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, value = "PEM")
        }
    )
    void testProduceConsumeOverPemTlsUnderFips(ClusterInstance cluster) throws Exception {
        FipsUtils.assertBrokerInFipsMode(cluster);

        String topicName = "fips-pem-tls-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }
}
