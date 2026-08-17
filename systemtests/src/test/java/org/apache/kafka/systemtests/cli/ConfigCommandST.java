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

package org.apache.kafka.systemtests.cli;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterSystemTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.systemtests.utils.cli.ContainerCommandUtils;

import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Container;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke tests for the kafka-configs.sh CLI tool executed inside containers.
 * Validates that configuration changes made through the CLI (the way real users
 * interact with Kafka) are correctly applied and visible through the Admin API.
 */
public class ConfigCommandST {

    @Timeout(120)
    @ClusterSystemTest(brokers = 1, controllers = 1, types = {Type.CO_KRAFT})
    void testAlterTopicConfig(ClusterInstance cluster) throws Exception {
        String topicName = "config-cli-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        Container.ExecResult result = ContainerCommandUtils.kafkaConfigs(cluster,
            "--alter",
            "--add-config", "compression.type=zstd",
            "--entity-type", "topics",
            "--entity-name", topicName);

        assertEquals(0, result.getExitCode(),
            "kafka-configs.sh --alter failed: " + result.getStderr());
        assertTrue(result.getStdout().contains("Completed updating config"),
            "Unexpected output: " + result.getStdout());

        try (Admin admin = cluster.admin()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Map<ConfigResource, org.apache.kafka.clients.admin.Config> configs =
                admin.describeConfigs(List.of(resource)).all().get();
            ConfigEntry entry = configs.get(resource).get("compression.type");
            assertNotNull(entry, "compression.type config entry should exist");
            assertEquals("zstd", entry.value(),
                "compression.type should be 'zstd' after CLI alter");
        }
    }

    @Timeout(120)
    @ClusterSystemTest(brokers = 1, controllers = 1, types = {Type.CO_KRAFT})
    void testDescribeTopicConfig(ClusterInstance cluster) throws Exception {
        String topicName = "config-describe-test-topic";
        cluster.createTopic(topicName, 1, (short) 1, Map.of("retention.ms", "86400000"));

        Container.ExecResult result = ContainerCommandUtils.kafkaConfigs(cluster,
            "--describe",
            "--entity-type", "topics",
            "--entity-name", topicName);

        assertEquals(0, result.getExitCode(),
            "kafka-configs.sh --describe failed: " + result.getStderr());
        assertTrue(result.getStdout().contains("retention.ms=86400000"),
            "Describe output should contain retention.ms=86400000, got: " + result.getStdout());
    }

    @Timeout(120)
    @ClusterSystemTest(brokers = 1, controllers = 1, types = {Type.CO_KRAFT})
    void testDeleteTopicConfig(ClusterInstance cluster) throws Exception {
        String topicName = "config-delete-test-topic";
        cluster.createTopic(topicName, 1, (short) 1, Map.of("retention.ms", "3600000"));

        Container.ExecResult result = ContainerCommandUtils.kafkaConfigs(cluster,
            "--alter",
            "--delete-config", "retention.ms",
            "--entity-type", "topics",
            "--entity-name", topicName);

        assertEquals(0, result.getExitCode(),
            "kafka-configs.sh --delete-config failed: " + result.getStderr());

        try (Admin admin = cluster.admin()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            ConfigEntry entry = admin.describeConfigs(List.of(resource))
                .all().get().get(resource).get("retention.ms");
            assertNotNull(entry, "retention.ms config entry should exist");
            assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, entry.source(),
                "retention.ms should revert to DEFAULT_CONFIG source after delete");
        }
    }

    @Timeout(120)
    @ClusterSystemTest(brokers = 3, controllers = 3, types = {Type.KRAFT})
    void testDescribeBrokerConfig(ClusterInstance cluster) throws Exception {
        int brokerId = cluster.brokerIds().iterator().next();

        Container.ExecResult result = ContainerCommandUtils.kafkaConfigs(cluster,
            "--describe",
            "--entity-type", "brokers",
            "--entity-name", String.valueOf(brokerId));

        assertEquals(0, result.getExitCode(),
            "kafka-configs.sh --describe broker failed: " + result.getStderr());
        assertTrue(result.getStdout().contains("Dynamic configs for broker " + brokerId),
            "Describe output should contain broker config header, got: " + result.getStdout());
    }

    @Timeout(120)
    @ClusterSystemTest(brokers = 1, controllers = 1, types = {Type.CO_KRAFT, Type.KRAFT})
    void testUncordonLogDirViaCli(ClusterInstance cluster) throws Exception {
        int brokerId = cluster.brokerIds().iterator().next();
        String logDir = "/var/lib/kafka/data";

        // Cordon the log dir via Admin API (forwarded through broker, so it's allowed)
        try (Admin admin = cluster.admin()) {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
            Collection<AlterConfigOp> ops = List.of(
                new AlterConfigOp(new ConfigEntry("cordoned.log.dirs", logDir), AlterConfigOp.OpType.SET)
            );
            admin.incrementalAlterConfigs(Map.of(brokerResource, ops)).all().get();
        }

        // Verify cordoned.log.dirs appears in CLI describe output
        Container.ExecResult describeResult = ContainerCommandUtils.kafkaConfigs(cluster,
            "--describe",
            "--entity-type", "brokers",
            "--entity-name", String.valueOf(brokerId));
        assertEquals(0, describeResult.getExitCode(),
            "kafka-configs.sh --describe failed: " + describeResult.getStderr());
        assertTrue(describeResult.getStdout().contains("cordoned.log.dirs"),
            "Describe output should contain cordoned.log.dirs after cordoning, got: " + describeResult.getStdout());

        // Uncordon via CLI (removes the dynamic config override, allowed without broker forwarding)
        Container.ExecResult deleteResult = ContainerCommandUtils.kafkaConfigs(cluster,
            "--alter",
            "--delete-config", "cordoned.log.dirs",
            "--entity-type", "brokers",
            "--entity-name", String.valueOf(brokerId));

        assertEquals(0, deleteResult.getExitCode(),
            "kafka-configs.sh --delete-config cordoned.log.dirs failed: " + deleteResult.getStderr());

        // Verify cordoned.log.dirs no longer appears in dynamic broker config
        Container.ExecResult afterDelete = ContainerCommandUtils.kafkaConfigs(cluster,
            "--describe",
            "--entity-type", "brokers",
            "--entity-name", String.valueOf(brokerId));
        assertEquals(0, afterDelete.getExitCode(),
            "kafka-configs.sh --describe after delete failed: " + afterDelete.getStderr());
        assertFalse(afterDelete.getStdout().contains("cordoned.log.dirs"),
            "cordoned.log.dirs should not appear after uncordoning via CLI, got: " + afterDelete.getStdout());
    }

    @Timeout(120)
    @ClusterSystemTest(brokers = 1, controllers = 1, types = {Type.CO_KRAFT, Type.KRAFT})
    void testDescribeCordonedLogDirsViaCli(ClusterInstance cluster) throws Exception {
        int brokerId = cluster.brokerIds().iterator().next();
        String logDir = "/var/lib/kafka/data";

        // Cordon the log dir via Admin API
        try (Admin admin = cluster.admin()) {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
            Collection<AlterConfigOp> ops = List.of(
                new AlterConfigOp(new ConfigEntry("cordoned.log.dirs", logDir), AlterConfigOp.OpType.SET)
            );
            admin.incrementalAlterConfigs(Map.of(brokerResource, ops)).all().get();
        }

        // Describe via CLI and verify cordoned.log.dirs appears as a dynamic broker config
        Container.ExecResult result = ContainerCommandUtils.kafkaConfigs(cluster,
            "--describe",
            "--entity-type", "brokers",
            "--entity-name", String.valueOf(brokerId));

        assertEquals(0, result.getExitCode(),
            "kafka-configs.sh --describe failed: " + result.getStderr());
        assertTrue(result.getStdout().contains("cordoned.log.dirs"),
            "Describe output should show cordoned.log.dirs in dynamic broker config, got: " + result.getStdout());
    }
}