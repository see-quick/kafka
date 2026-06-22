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
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.ExecutionMode;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

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

        // Produce first batch of messages
        produceMessages(cluster, topicName, 0, NUM_MESSAGES);

        // Rolling restart: stop and start each broker
        for (int brokerId : cluster.brokerIds()) {
            cluster.shutdownBroker(brokerId);
            cluster.startBroker(brokerId);
            cluster.waitForReadyBrokers();
        }

        // Produce second batch of messages after bounce
        produceMessages(cluster, topicName, NUM_MESSAGES, NUM_MESSAGES);

        // Consume all messages and verify count
        try (Consumer<String, String> consumer = cluster.consumer(Map.of(
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))
        ) {
            consumer.subscribe(List.of(topicName));
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            int expectedTotal = NUM_MESSAGES * 2;
            // After a bounce the group coordinator needs time to reload __consumer_offsets
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
                return records.size() >= expectedTotal;
            }, 60_000L, "Failed to consume all " + expectedTotal + " messages after bounce");

            assertEquals(expectedTotal, records.size());
        }
    }

    private void produceMessages(ClusterInstance cluster, String topicName, int startIndex, int count) {
        try (Producer<String, String> producer = cluster.producer(Map.of(
                ACKS_CONFIG, "all",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))
        ) {
            for (int i = startIndex; i < startIndex + count; i++) {
                producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i));
            }
            producer.flush();
        }
    }
}
