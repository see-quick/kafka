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

package org.apache.kafka.systemtests.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests produce/consume for all compression types: snappy, gzip, lz4, zstd, none.
 * Each compression type is tested with a separate cluster config using tags to carry
 * the compression type to the test method.
 */
@ClusterTestDefaults(executionModes = {ExecutionMode.CONTAINER})
public class CompressionST {

    private static final int NUM_MESSAGES = 1000;
    private static final int NUM_PARTITIONS = 10;

    @ClusterTests({
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=snappy"}),
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=gzip"}),
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=lz4"}),
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=zstd"}),
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=none"})
    })
    void testCompressedTopic(ClusterInstance cluster) throws InterruptedException {
        String compressionType = cluster.config().tags().stream()
            .filter(t -> t.startsWith("compression="))
            .map(t -> t.substring("compression=".length()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Missing compression tag"));

        String topicName = "compression-test-" + compressionType;
        cluster.createTopic(topicName, NUM_PARTITIONS, (short) 1);

        // Produce messages with the specified compression type
        try (Producer<String, String> producer = cluster.producer(Map.of(
                ACKS_CONFIG, "all",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType))
        ) {
            for (int i = 0; i < NUM_MESSAGES; i++) {
                producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i));
            }
            producer.flush();
        }

        // Consume and verify all messages
        try (Consumer<String, String> consumer = cluster.consumer(Map.of(
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))
        ) {
            consumer.subscribe(List.of(topicName));
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
                return records.size() >= NUM_MESSAGES;
            }, 60_000L, "Failed to consume all " + NUM_MESSAGES + " messages with compression=" + compressionType);

            assertEquals(NUM_MESSAGES, records.size(),
                "Expected " + NUM_MESSAGES + " messages with compression=" + compressionType);
        }
    }
}
