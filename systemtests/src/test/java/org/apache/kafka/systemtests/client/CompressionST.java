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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

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

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES, compressionType);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, NUM_PARTITIONS, NUM_MESSAGES);

        assertEquals(NUM_MESSAGES, records.size(),
            "Expected " + NUM_MESSAGES + " messages with compression=" + compressionType);
    }

    @Timeout(120)
    @ClusterTest(types = {Type.KRAFT}, controllers = 5, brokers = 5)
    void testAllCompressionsConcurrently(ClusterInstance cluster) throws Exception {
        String topicName = "compression-concurrent-test";
        cluster.createTopic(topicName, NUM_PARTITIONS, (short) 1);

        int expectedTotal = NUM_MESSAGES * COMPRESSION_TYPES.length;
        AtomicInteger producedCount = new AtomicInteger(0);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String compressionType : COMPRESSION_TYPES) {
            futures.add(CompletableFuture.runAsync(() -> {
                ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES, compressionType);
                producedCount.addAndGet(NUM_MESSAGES);
            }));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        assertEquals(expectedTotal, producedCount.get(), "Not all producers completed successfully");

        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, NUM_PARTITIONS, expectedTotal);

        assertEquals(expectedTotal, records.size(),
            "Expected " + expectedTotal + " messages from " + COMPRESSION_TYPES.length + " concurrent producers");
    }
}
