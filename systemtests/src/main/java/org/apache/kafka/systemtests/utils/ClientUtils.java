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

package org.apache.kafka.systemtests.utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public final class ClientUtils {

    private static final long DEFAULT_CONSUME_TIMEOUT_MS = 60_000L;

    private ClientUtils() {
    }

    public static void produceMessages(ClusterInstance cluster, String topicName, int count) {
        produceMessages(cluster, topicName, 0, count, Map.of());
    }

    public static void produceMessages(ClusterInstance cluster, String topicName,
                                       int startIndex, int count) {
        produceMessages(cluster, topicName, startIndex, count, Map.of());
    }

    public static void produceMessages(ClusterInstance cluster, String topicName,
                                       int count, String compressionType) {
        produceMessages(cluster, topicName, 0, count,
            Map.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType));
    }

    public static void produceMessages(ClusterInstance cluster, String topicName,
                                       int startIndex, int count,
                                       Map<String, String> extraProducerConfig) {
        Map<String, String> config = new HashMap<>();
        config.put(ACKS_CONFIG, "all");
        config.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.putAll(extraProducerConfig);

        try (Producer<String, String> producer = cluster.producer(Map.copyOf(config))) {
            for (int i = startIndex; i < startIndex + count; i++) {
                producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i));
            }
            producer.flush();
        }
    }

    public static List<ConsumerRecord<String, String>> consumeMessages(
            ClusterInstance cluster, String topicName, int numPartitions,
            int expectedCount) throws InterruptedException {
        return consumeMessages(cluster, topicName, numPartitions, expectedCount,
            DEFAULT_CONSUME_TIMEOUT_MS);
    }

    public static List<ConsumerRecord<String, String>> consumeMessages(
            ClusterInstance cluster, String topicName, int numPartitions,
            int expectedCount, long timeoutMs) throws InterruptedException {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (Consumer<String, String> consumer = cluster.consumer(Map.of(
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))
        ) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(new TopicPartition(topicName, i));
            }
            consumer.assign(partitions);
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
                return records.size() >= expectedCount;
            }, timeoutMs,
                "Failed to consume " + expectedCount + " messages from " + topicName
                    + " (got " + records.size() + ")");
        }
        return records;
    }
}
