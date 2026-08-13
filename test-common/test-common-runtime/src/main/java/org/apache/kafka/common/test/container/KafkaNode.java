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

package org.apache.kafka.common.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;

/**
 * A Kafka container node that injects the correct advertised listeners
 * (with Docker-mapped host port) via a startup script before Kafka starts.
 */
class KafkaNode extends GenericContainer<KafkaNode> {
    private final int nodeId;
    private final KafkaNodeRole role;

    KafkaNode(DockerImageName image, int nodeId, KafkaNodeRole role) {
        super(image);
        this.nodeId = nodeId;
        this.role = role;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo) {
        super.containerIsStarting(containerInfo);

        StringBuilder script = new StringBuilder("#!/bin/bash\n");
        if (role.isBroker()) {
            int mappedPort = this.getMappedPort(KafkaContainerCluster.KAFKA_PORT);
            script.append("export ")
                .append(KafkaEnvVars.ADVERTISED_LISTENERS)
                .append("='").append(Listener.EXTERNAL).append("://localhost:")
                .append(mappedPort)
                .append(",").append(Listener.INTERNAL).append("://kafka-")
                .append(nodeId).append(":").append(KafkaContainerCluster.INTERNAL_PORT)
                .append("'\n");
        }
        script.append("exec /etc/kafka/docker/run\n");
        copyFileToContainer(
            Transferable.of(script.toString().getBytes(StandardCharsets.UTF_8), 0777),
            KafkaContainerCluster.STARTER_SCRIPT);
    }
}