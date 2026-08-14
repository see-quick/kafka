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

package org.apache.kafka.systemtests.utils.cli;

import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.container.ContainerClusterInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class ContainerCommandUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerCommandUtils.class);

    private static final String BIN_DIR = "/opt/kafka/bin/";
    private static final int INTERNAL_PORT = 9094;

    private ContainerCommandUtils() {
    }

    public static Container.ExecResult runCommand(ClusterInstance cluster, String... command)
            throws IOException, InterruptedException {
        if (!(cluster instanceof ContainerClusterInstance)) {
            throw new IllegalArgumentException(
                "CLI commands can only be executed on container-based clusters, " +
                "but got " + cluster.getClass().getSimpleName());
        }

        ContainerClusterInstance containerCluster = (ContainerClusterInstance) cluster;
        GenericContainer<?> broker = containerCluster.containers().entrySet().stream()
            .filter(e -> cluster.brokerIds().contains(e.getKey()))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No broker container found"));

        LOG.info("Executing command in container: {}", Arrays.toString(command));
        Container.ExecResult result = broker.execInContainer(command);
        LOG.info("Command exit code: {}, stdout: {}, stderr: {}",
            result.getExitCode(), result.getStdout().trim(), result.getStderr().trim());
        return result;
    }

    public static Container.ExecResult kafkaConfigs(ClusterInstance cluster, String... args)
            throws IOException, InterruptedException {
        return runKafkaTool(cluster, "kafka-configs.sh", args);
    }

    public static Container.ExecResult kafkaTopics(ClusterInstance cluster, String... args)
            throws IOException, InterruptedException {
        return runKafkaTool(cluster, "kafka-topics.sh", args);
    }

    private static Container.ExecResult runKafkaTool(ClusterInstance cluster,
                                                     String script,
                                                     String... args)
            throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(BIN_DIR + script);
        command.add("--bootstrap-server");
        command.add("localhost:" + INTERNAL_PORT);
        command.addAll(Arrays.asList(args));
        return runCommand(cluster, command.toArray(new String[0]));
    }
}