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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests produce/consume for all compression types: snappy, gzip, lz4, zstd, none.
 *
 * <p>Two test variants:
 * <ul>
 *   <li>{@code testCompressedTopic} -- each compression type tested in isolation (one per invocation)</li>
 *   <li>{@code testAllCompressionsConcurrently} -- all compression types produced concurrently
 *       to the same topic, mirroring the Ducktape CompressionTest behavior</li>
 * </ul>
 */
@Tag("system")
@ClusterTestDefaults(executionModes = {ExecutionMode.CONTAINER})
public class CompressionST {

    private static final String[] COMPRESSION_TYPES = {"snappy", "gzip", "lz4", "zstd", "none"};
    private static final int NUM_MESSAGES = 1000;
    private static final int NUM_PARTITIONS = 10;

    // --- Per-compression-type isolated tests ---

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

        produceMessages(cluster, topicName, compressionType, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = consumeMessages(cluster, topicName, NUM_MESSAGES,
            "Failed to consume all " + NUM_MESSAGES + " messages with compression=" + compressionType);

        assertEquals(NUM_MESSAGES, records.size(),
            "Expected " + NUM_MESSAGES + " messages with compression=" + compressionType);
    }

    // --- Concurrent all-compression test (mirrors Ducktape CompressionTest) ---

    @Timeout(120)
    @ClusterTest(types = {Type.KRAFT}, controllers = 5, brokers = 5)
    void testAllCompressionsConcurrently(ClusterInstance cluster) throws Exception {
        String topicName = "compression-concurrent-test";
        cluster.createTopic(topicName, NUM_PARTITIONS, (short) 1);

        int expectedTotal = NUM_MESSAGES * COMPRESSION_TYPES.length;
        AtomicInteger producedCount = new AtomicInteger(0);

        // Launch one producer per compression type, all producing concurrently to the same topic
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String compressionType : COMPRESSION_TYPES) {
            futures.add(CompletableFuture.runAsync(() -> {
                produceMessages(cluster, topicName, compressionType, NUM_MESSAGES);
                producedCount.addAndGet(NUM_MESSAGES);
            }));
        }

        // Wait for all producers to finish
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        assertEquals(expectedTotal, producedCount.get(), "Not all producers completed successfully");

        // Consume all messages from all compression types
        List<ConsumerRecord<String, String>> records = consumeMessages(cluster, topicName, expectedTotal,
            "Failed to consume all " + expectedTotal + " messages from concurrent compression producers");

        assertEquals(expectedTotal, records.size(),
            "Expected " + expectedTotal + " messages from " + COMPRESSION_TYPES.length + " concurrent producers");
    }

    // --- Helpers ---

    private void produceMessages(ClusterInstance cluster, String topicName, String compressionType, int count) {
        try (Producer<String, String> producer = cluster.producer(Map.of(
                ACKS_CONFIG, "all",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType))
        ) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topicName, compressionType + "-key-" + i, "value-" + i));
            }
            producer.flush();
        }
    }

    private List<ConsumerRecord<String, String>> consumeMessages(
            ClusterInstance cluster, String topicName, int expectedCount, String errorMessage)
            throws InterruptedException {
        List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        try (Consumer<String, String> consumer = cluster.consumer(Map.of(
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))
        ) {
            consumer.subscribe(List.of(topicName));
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
                return records.size() >= expectedCount;
            }, 60_000L, errorMessage);
        }
        return records;
    }
}