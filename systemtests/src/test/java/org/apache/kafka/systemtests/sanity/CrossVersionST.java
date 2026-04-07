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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ExecutionMode;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.systemtests.KafkaVersions;

import org.junit.jupiter.api.Timeout;
import org.testcontainers.shaded.org.bouncycastle.pqc.legacy.math.linearalgebra.Matrix;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Sanity test that verifies basic produce/consume works against each released Kafka version.
 * Uses the current client against broker containers of older versions.
 *
 * // TODO: just to show how it could be possibly handless and centrally modified in the KafkaVersions :))
 */
@ClusterTestDefaults(executionModes = {ExecutionMode.CONTAINER})
public class CrossVersionST {

    private static final int NUM_MESSAGES = 100;

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
        cluster.createTopic(topicName, 3, (short) 1);

        // Verify admin can describe the cluster
        try (Admin admin = cluster.admin()) {
            assertFalse(admin.describeCluster().nodes().get().isEmpty(),
                "Should describe cluster for " + version);
        } catch (Exception e) {
            throw new RuntimeException("Failed to describe cluster for " + version, e);
        }

        // Produce messages
        try (Producer<String, String> producer = cluster.producer(Map.of(
                ACKS_CONFIG, "all",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))
        ) {
            for (int i = 0; i < NUM_MESSAGES; i++) {
                producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i));
            }
            producer.flush();
        }

        // Consume and verify all messages
        try (Consumer<String, String> consumer = cluster.consumer(Map.of(
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))
        ) {
            consumer.subscribe(List.of(topicName));
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
                return records.size() >= NUM_MESSAGES;
            }, 30_000L, "Failed to consume all " + NUM_MESSAGES + " messages from " + version);

            assertEquals(NUM_MESSAGES, records.size(),
                "Expected " + NUM_MESSAGES + " messages from " + version);
        }
    }
}