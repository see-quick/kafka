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

package org.apache.kafka.systemtests.sanity;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.ExecutionMode;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.systemtests.utils.ClientUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests cluster rolling restart with produce before/after bounce.
 * Verifies that messages produced before and after a rolling restart
 * are all consumable.
 */
@Tag("system")
@ClusterTestDefaults(executionModes = {ExecutionMode.CONTAINER})
public class BounceST {

    private static final int NUM_MESSAGES = 1000;

    @Timeout(300)
    @ClusterTests({
        @ClusterTest(brokers = 1, controllers = 1, types = {Type.KRAFT, Type.CO_KRAFT}),
        @ClusterTest(brokers = 3, controllers = 3, types = {Type.KRAFT, Type.CO_KRAFT})
    })
    void testBounce(ClusterInstance cluster) throws InterruptedException {
        String topicName = "bounce-test-topic";
        short replicationFactor = (short) Math.min(cluster.config().numBrokers(), 3);
        cluster.createTopic(topicName, 1, replicationFactor);

        ClientUtils.produceMessages(cluster, topicName, 0, NUM_MESSAGES);

        for (int brokerId : cluster.brokerIds()) {
            cluster.shutdownBroker(brokerId);
            cluster.startBroker(brokerId);
            cluster.waitForReadyBrokers();
        }

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES, NUM_MESSAGES);

        int expectedTotal = NUM_MESSAGES * 2;
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, expectedTotal);

        assertEquals(expectedTotal, records.size());
    }
}
