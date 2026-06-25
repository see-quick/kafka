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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ExecutionMode;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.systemtests.KafkaVersions;
import org.apache.kafka.systemtests.utils.ClientUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Sanity test that verifies basic produce/consume works against each released Kafka version.
 * Uses the current client against broker containers of older versions.
 */
@Tag("system")
@ClusterTestDefaults(executionModes = {ExecutionMode.CONTAINER})
public class CrossVersionST {

    private static final int NUM_MESSAGES = 100;
    private static final int NUM_PARTITIONS = 3;

    static String[] crossVersionImages() {
        return KafkaVersions.crossVersionImages();
    }

    @Timeout(120)
    @ClusterTest(
        types = {Type.KRAFT, Type.CO_KRAFT},
        containerImageSource = "crossVersionImages"
    )
    void testProduceConsume(ClusterInstance cluster) throws InterruptedException {
        String version = cluster.config().containerImage().orElse("unknown");

        String topicName = "cross-version-test";
        cluster.createTopic(topicName, NUM_PARTITIONS, (short) 1);

        try (Admin admin = cluster.admin()) {
            assertFalse(admin.describeCluster().nodes().get().isEmpty(),
                "Should describe cluster for " + version);
        } catch (Exception e) {
            throw new RuntimeException("Failed to describe cluster for " + version, e);
        }

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);

        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, NUM_PARTITIONS, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size(),
            "Expected " + NUM_MESSAGES + " messages from " + version);
    }
}
