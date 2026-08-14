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

import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ExecutionMode;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.test.junit.RaftClusterInvocationContext;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.fault.FaultHandlerException;

import org.testcontainers.containers.GenericContainer;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link ClusterInstance} backed by Docker containers via testcontainers.
 * This provides only client-accessible operations. Methods that require access to in-process
 * server internals (e.g., {@link #brokers()}, {@link #controllers()}) throw
 * {@link UnsupportedOperationException}.
 */
public class ContainerClusterInstance implements ClusterInstance {

    private final ClusterConfig clusterConfig;
    private final KafkaContainerCluster cluster;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final ListenerName listenerName;
    private final boolean isCombined;

    public ContainerClusterInstance(ClusterConfig clusterConfig, boolean isCombined) {
        this.clusterConfig = clusterConfig;
        this.isCombined = isCombined;
        this.listenerName = clusterConfig.brokerListenerName();
        KafkaNodeConfig nodeConfig = KafkaNodeConfig.builder()
            .securityProtocol(clusterConfig.brokerSecurityProtocol())
            .saslMechanism(clusterConfig.saslMechanism())
            .serverProperties(clusterConfig.serverProperties())
            .build();
        this.cluster = new KafkaContainerCluster(
            clusterConfig.numBrokers(),
            clusterConfig.numControllers(),
            isCombined,
            nodeConfig,
            clusterConfig.containerImage().orElse(null)
        );
    }

    @Override
    public Type type() {
        return isCombined ? Type.CO_KRAFT : Type.KRAFT;
    }

    @Override
    public ExecutionMode executionMode() {
        return ExecutionMode.CONTAINER;
    }

    @Override
    public ClusterConfig config() {
        return clusterConfig;
    }

    @Override
    public Set<Integer> controllerIds() {
        return cluster.controllerIds();
    }

    @Override
    public Set<Integer> brokerIds() {
        return cluster.brokerIds();
    }

    @Override
    public ListenerName clientListener() {
        return listenerName;
    }

    @Override
    public ListenerName controllerListenerName() {
        return clusterConfig.controllerListenerName();
    }

    @Override
    public String bootstrapServers() {
        return cluster.bootstrapServers();
    }

    @Override
    public String bootstrapControllers() {
        return cluster.bootstrapControllers();
    }

    @Override
    public String clusterId() {
        return cluster.clusterId();
    }

    public Map<Integer, GenericContainer<?>> containers() {
        return cluster.containers();
    }

    public void setLogDir(Path logDir) {
        cluster.setLogDir(logDir);
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            cluster.start();
        }
    }

    @Override
    public boolean started() {
        return started.get();
    }

    @Override
    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            cluster.stop();
        }
    }

    @Override
    public boolean stopped() {
        return stopped.get();
    }

    @Override
    public void shutdownBroker(int brokerId) {
        cluster.stopBroker(brokerId);
    }

    @Override
    public void startBroker(int brokerId) {
        cluster.startBroker(brokerId);
    }

    @Override
    public void restartBroker(int brokerId, Map<String, Object> propOverrides) {
        throw new UnsupportedOperationException("Not yet implemented for container-based clusters");
    }

    @Override
    public void restartBrokersWithSwappedClientListenerPorts(int brokerId1, int brokerId2) {
        throw new UnsupportedOperationException("Not yet implemented for container-based clusters");
    }

    @Override
    public void waitForReadyBrokers() throws InterruptedException {
        try (Admin admin = admin()) {
            RaftClusterInvocationContext.waitForCondition(
                () -> {
                    try {
                        int readyBrokers = admin.describeCluster().nodes().get().size();
                        return readyBrokers >= clusterConfig.numBrokers();
                    } catch (InterruptedException | ExecutionException e) {
                        return false;
                    }
                },
                "Brokers did not become ready",
                120_000L
            );
        }
    }

    @Override
    public Map<String, Object> setClientSaslConfig(Map<String, Object> configs) {
        if (clusterConfig.brokerSecurityProtocol() == SecurityProtocol.PLAINTEXT) {
            return configs;
        }
        throw new UnsupportedOperationException(
            "SASL/SSL client configuration is not yet implemented for container-based clusters");
    }

    @Override
    public void waitTopicCreation(String topic, int partitions) throws InterruptedException {
        if (partitions <= 0) {
            throw new IllegalArgumentException("Partition count must be > 0, but was " + partitions);
        }

        try (Admin admin = admin()) {
            RaftClusterInvocationContext.waitForCondition(() -> {
                try {
                    TopicDescription desc = admin.describeTopics(List.of(topic))
                        .topicNameValues().get(topic).get();
                    return desc.partitions().size() == partitions;
                } catch (InterruptedException | ExecutionException e) {
                    return false;
                }
            }, topic + " not created with " + partitions + " partitions after timeout");
        }
    }

    @Override
    public List<Integer> controllerBoundPorts() {
        throw new UnsupportedOperationException("Not yet implemented for container-based clusters");
    }

    @Override
    public List<Integer> brokerBoundPorts() {
        throw new UnsupportedOperationException("Not yet implemented for container-based clusters");
    }

    @Override
    public Map<String, Object> setClientSslConfig(Map<String, Object> configs) {
        if (clusterConfig.brokerSecurityProtocol() == SecurityProtocol.PLAINTEXT) {
            return configs;
        }
        throw new UnsupportedOperationException(
            "SSL client configuration is not yet implemented for container-based clusters");
    }

    @Override
    public Set<GroupProtocol> supportedGroupProtocols() {
        throw new UnsupportedOperationException("Not yet implemented for container-based clusters");
    }

    @Override
    public void waitTopicDeletion(String topic) {
        throw new UnsupportedOperationException("Not yet implemented for container-based clusters");
    }

    @Override
    public void ensureConsistentMetadata() {
        throw new UnsupportedOperationException("Not yet implemented for container-based clusters");
    }

    @Override
    public void ensureConsistentMetadata(Collection<KafkaBroker> brokers, Collection<ControllerServer> controllers) {
        throw new UnsupportedOperationException("Not yet implemented for container-based clusters");
    }

    @Override
    public Map<Integer, KafkaBroker> brokers() {
        throw new UnsupportedOperationException(
            "Requires in-process access to KafkaBroker instances; not available in container-based clusters");
    }

    @Override
    public Map<Integer, ControllerServer> controllers() {
        throw new UnsupportedOperationException(
            "Requires in-process access to ControllerServer instances; not available in container-based clusters");
    }

    @Override
    public Map<Integer, KafkaBroker> aliveBrokers() {
        throw new UnsupportedOperationException(
            "Requires in-process access to KafkaBroker instances; not available in container-based clusters");
    }

    @Override
    public Optional<FaultHandlerException> firstFatalException() {
        throw new UnsupportedOperationException(
            "Requires in-process access to fault handlers; not available in container-based clusters");
    }

    @Override
    public Optional<FaultHandlerException> firstNonFatalException() {
        throw new UnsupportedOperationException(
            "Requires in-process access to fault handlers; not available in container-based clusters");
    }

    @Override
    public List<Authorizer> authorizers() {
        throw new UnsupportedOperationException(
            "Requires in-process access to Authorizer instances; not available in container-based clusters");
    }

    @Override
    public void waitAcls(AclBindingFilter filter, Collection<AccessControlEntry> entries) {
        throw new UnsupportedOperationException(
            "Requires in-process access to Authorizer instances; not available in container-based clusters");
    }
}
