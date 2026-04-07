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

package org.apache.kafka.common.test;

import com.github.dockerjava.api.command.InspectContainerResponse;

import org.apache.kafka.common.security.auth.SecurityProtocol;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.DockerImageName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Manages a Kafka cluster running inside Docker containers via testcontainers.
 * Each node runs the repo-built Kafka Docker image using KRaft mode (no ZooKeeper).
 *
 * <p>This class handles:
 * <ul>
 *   <li>Container lifecycle (start, stop, per-broker stop/start)</li>
 *   <li>KRaft quorum configuration (process.roles, node.id, controller.quorum.voters)</li>
 *   <li>Listener configuration for inter-container and host-to-container communication</li>
 *   <li>Cluster formation across multiple containers on a shared Docker network</li>
 *   <li>SASL security protocol configuration for the client-facing listener</li>
 * </ul>
 *
 * <p>Advertised listeners are configured dynamically at container startup using
 * the testcontainers {@code containerIsStarting} lifecycle hook, which injects
 * a startup script with the correct Docker-mapped port before Kafka starts.
 */
public class KafkaContainerCluster implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerCluster.class);

    private static final String DEFAULT_IMAGE = "apache/kafka:latest";
    private static final String IMAGE_PROPERTY = "kafka.container.image";
    private static final String STARTER_SCRIPT = "/tmp/start_kafka.sh";
    private static final int KAFKA_PORT = 9092;
    private static final int CONTROLLER_PORT = 9093;
    private static final int INTERNAL_PORT = 9094;

    private final int numBrokers;
    private final int numControllers;
    private final boolean combined;
    private final Map<String, String> serverProperties;
    private final SecurityProtocol securityProtocol;
    private final String saslMechanism;
    private final Network network;
    private final Map<Integer, GenericContainer<?>> containers;
    // Tracks which nodes are running internally rather than querying the Docker API
    // via container.isRunning(). After a Docker stop/start cycle the Docker daemon
    // can become temporarily unresponsive (NoHttpResponseException on localhost:2375),
    // which would cause bootstrapServers()/bootstrapControllers() to incorrectly
    // exclude brokers that are actually running.
    private final Set<Integer> runningNodeIds;
    private final DockerImageName imageName;

    @SuppressWarnings("resource")
    public KafkaContainerCluster(int numBrokers, int numControllers, boolean combined,
                                  Map<String, String> serverProperties,
                                  SecurityProtocol securityProtocol, String saslMechanism,
                                  String containerImage) {
        this.numBrokers = numBrokers;
        this.numControllers = numControllers;
        this.combined = combined;
        this.serverProperties = new HashMap<>(serverProperties);
        this.securityProtocol = securityProtocol;
        this.saslMechanism = saslMechanism;
        this.network = Network.newNetwork();
        this.containers = new TreeMap<>();
        this.runningNodeIds = ConcurrentHashMap.newKeySet();

        String imageTag = containerImage != null
            ? containerImage
            : System.getProperty(IMAGE_PROPERTY, DEFAULT_IMAGE);
        this.imageName = DockerImageName.parse(imageTag);

        createContainers();
    }

    @SuppressWarnings("resource")
    private void createContainers() {
        String quorumVoters = buildQuorumVoters();
        int totalNodes = combined ? Math.max(numBrokers, numControllers) : numBrokers + numControllers;

        for (int nodeId = 0; nodeId < totalNodes; nodeId++) {
            String processRoles = getProcessRoles(nodeId);
            Map<String, String> config = buildNodeConfig(nodeId, processRoles, quorumVoters);

            KafkaNode container = new KafkaNode(imageName, nodeId, processRoles);

            container
                .withNetwork(network)
                .withNetworkAliases("kafka-" + nodeId)
                .withEnv(toEnvVars(config));

            if (processRoles.contains("broker") && processRoles.contains("controller")) {
                container.withExposedPorts(KAFKA_PORT, CONTROLLER_PORT);
            } else if (processRoles.contains("broker")) {
                container.withExposedPorts(KAFKA_PORT);
            } else {
                // Controller-only node
                container.withExposedPorts(CONTROLLER_PORT);
            }

            container
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka-container-" + nodeId)))
                .withCommand("sh", "-c",
                    "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT)
                .waitingFor(Wait.forListeningPort()
                    .withStartupTimeout(Duration.ofMinutes(2)));

            containers.put(nodeId, container);
        }
    }

    private String getProcessRoles(int nodeId) {
        if (combined) {
            boolean isBroker = nodeId < numBrokers;
            boolean isController = nodeId < numControllers;
            if (isBroker && isController) return "broker,controller";
            if (isBroker) return "broker";
            return "controller";
        } else {
            // Isolated: controllers first, then brokers
            if (nodeId < numControllers) return "controller";
            return "broker";
        }
    }

    private String buildQuorumVoters() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numControllers; i++) {
            if (!sb.isEmpty()) sb.append(",");
            sb.append(i).append("@kafka-").append(i).append(":").append(CONTROLLER_PORT);
        }
        return sb.toString();
    }

    private Map<String, String> buildNodeConfig(int nodeId, String processRoles, String quorumVoters) {
        Map<String, String> config = new HashMap<>();

        config.put("KAFKA_NODE_ID", String.valueOf(nodeId));
        config.put("KAFKA_PROCESS_ROLES", processRoles);
        config.put("KAFKA_CONTROLLER_QUORUM_VOTERS", quorumVoters);
        config.put("CLUSTER_ID", "test-container-cluster-id-01");

        // Listeners configuration
        if (processRoles.contains("broker")) {
            config.put("KAFKA_LISTENERS",
                "EXTERNAL://0.0.0.0:" + KAFKA_PORT +
                ",INTERNAL://0.0.0.0:" + INTERNAL_PORT +
                (processRoles.contains("controller") ? ",CONTROLLER://0.0.0.0:" + CONTROLLER_PORT : ""));

            // KAFKA_ADVERTISED_LISTENERS is NOT set here — it is injected by the
            // containerIsStarting() hook with the correct Docker-mapped host port.

            config.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "EXTERNAL:" + securityProtocol.name() + ",INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");
            config.put("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL");
        } else {
            // Controller-only node
            config.put("KAFKA_LISTENERS", "CONTROLLER://0.0.0.0:" + CONTROLLER_PORT);
            config.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT");
        }

        config.put("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");

        // Use the volume-backed data directory instead of the default /tmp/kafka-logs
        config.put("KAFKA_LOG_DIRS", "/var/lib/kafka/data");

        // Default config
        config.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", String.valueOf(Math.min(numBrokers, 3)));
        config.put("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        config.put("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        config.put("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", String.valueOf(Math.min(numBrokers, 3)));

        // SASL configuration (only for broker nodes with SASL protocols)
        if (processRoles.contains("broker") && isSaslProtocol()) {
            config.put("KAFKA_SASL_ENABLED_MECHANISMS", saslMechanism);
            if ("PLAIN".equals(saslMechanism)) {
                // For PLAIN, configure the JAAS config inline via env var.
                // The env var name encodes listener name + mechanism:
                //   KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG
                String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + JaasUtils.KAFKA_PLAIN_ADMIN + "\" "
                    + "password=\"" + JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD + "\" "
                    + "user_" + JaasUtils.KAFKA_PLAIN_ADMIN + "=\"" + JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD + "\" "
                    + "user_" + JaasUtils.KAFKA_PLAIN_USER1 + "=\"" + JaasUtils.KAFKA_PLAIN_USER1_PASSWORD + "\";";
                config.put("KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG", jaasConfig);
            } else if (saslMechanism != null && saslMechanism.startsWith("SCRAM-")) {
                // For SCRAM, the broker-side JAAS config just needs the login module.
                // Users are stored in metadata and created via kafka-configs after startup.
                // The double underscores in env var names represent hyphens in the mechanism name.
                String envMechanism = saslMechanism.replace("-", "__");
                String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required;";
                config.put("KAFKA_LISTENER_NAME_EXTERNAL_" + envMechanism + "_SASL_JAAS_CONFIG", jaasConfig);
            }
        }

        // Apply user-provided server properties
        for (Map.Entry<String, String> entry : serverProperties.entrySet()) {
            config.put("KAFKA_" + entry.getKey().replace(".", "_").toUpperCase(Locale.ROOT), entry.getValue());
        }

        return config;
    }

    private boolean isSaslProtocol() {
        return securityProtocol == SecurityProtocol.SASL_PLAINTEXT
            || securityProtocol == SecurityProtocol.SASL_SSL;
    }

    private Map<String, String> toEnvVars(Map<String, String> config) {
        return Collections.unmodifiableMap(config);
    }

    /**
     * Starts all containers in the cluster in parallel.
     *
     * <p>Parallel startup is required for multi-node KRaft clusters: each node's
     * broker port only opens after a controller quorum forms, which requires a
     * majority of controllers to be running simultaneously. Sequential startup
     * would cause the first node to timeout waiting for a quorum that can never form.
     *
     * <p>Each container's advertised listeners are configured automatically via
     * the {@code containerIsStarting} lifecycle hook before Kafka starts.
     */
    public void start() {
        LOG.info("Starting Kafka container cluster with {} brokers and {} controllers (combined={}, protocol={})",
            numBrokers, numControllers, combined, securityProtocol);

        try {
            Startables.deepStart(containers.values().stream()).get(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while starting Kafka containers", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to start Kafka containers", e);
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out while starting Kafka containers", e);
        }

        runningNodeIds.addAll(containers.keySet());

        // For SCRAM mechanisms, create the test users via kafka-configs after the cluster is up
        if (isSaslProtocol() && saslMechanism != null && saslMechanism.startsWith("SCRAM-")) {
            createScramUsers();
        }

        LOG.info("Kafka container cluster started. Bootstrap servers: {}", bootstrapServers());
    }

    private void createScramUsers() {
        // Pick any broker container to run the kafka-configs command
        GenericContainer<?> brokerContainer = containers.entrySet().stream()
            .filter(e -> getProcessRoles(e.getKey()).contains("broker"))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No broker container found"));

        // Create admin user
        createScramUser(brokerContainer, JaasUtils.KAFKA_PLAIN_ADMIN, JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD);
        // Create test user
        createScramUser(brokerContainer, JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD);
    }

    private void createScramUser(GenericContainer<?> container, String username, String password) {
        try {
            org.testcontainers.containers.Container.ExecResult result = container.execInContainer(
                "/opt/kafka/bin/kafka-configs.sh",
                "--bootstrap-server", "localhost:" + INTERNAL_PORT,
                "--alter",
                "--add-config", saslMechanism + "=[password=" + password + ",iterations=4096]",
                "--entity-type", "users",
                "--entity-name", username
            );
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Failed to create SCRAM user " + username
                    + ": " + result.getStderr());
            }
            LOG.info("Created SCRAM user: {}", username);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SCRAM user " + username, e);
        }
    }

    /**
     * Stops all containers in the cluster.
     */
    public void stop() {
        LOG.info("Stopping Kafka container cluster");
        runningNodeIds.clear();
        for (GenericContainer<?> container : containers.values()) {
            try {
                container.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping container", e);
            }
        }
        try {
            network.close();
        } catch (Exception e) {
            LOG.warn("Error closing network", e);
        }
    }

    /**
     * Stops a specific broker by node ID.
     * Uses Docker stop (not testcontainers stop) to preserve the container
     * filesystem so that log data survives across restarts.
     */
    public void stopBroker(int nodeId) {
        GenericContainer<?> container = containers.get(nodeId);
        if (container == null) {
            throw new IllegalArgumentException("Unknown nodeId " + nodeId);
        }
        LOG.info("Stopping broker {}", nodeId);
        DockerClientFactory.lazyClient().stopContainerCmd(container.getContainerId()).exec();
        runningNodeIds.remove(nodeId);
    }

    /**
     * Restarts a previously stopped broker.
     * Uses Docker start to restart the same container, preserving log data
     * and port mappings from the original start.
     */
    public void startBroker(int nodeId) {
        GenericContainer<?> container = containers.get(nodeId);
        if (container == null) {
            throw new IllegalArgumentException("Unknown nodeId " + nodeId);
        }

        if (runningNodeIds.contains(nodeId)) {
            return;
        }

        LOG.info("Starting broker {}", nodeId);
        DockerClientFactory.lazyClient().startContainerCmd(container.getContainerId()).exec();
        runningNodeIds.add(nodeId);
    }

    /**
     * Returns the bootstrap servers string for client connections.
     */
    public String bootstrapServers() {
        return containers.entrySet().stream()
            .filter(e -> getProcessRoles(e.getKey()).contains("broker"))
            .filter(e -> runningNodeIds.contains(e.getKey()))
            .map(e -> "localhost:" + e.getValue().getMappedPort(KAFKA_PORT))
            .collect(Collectors.joining(","));
    }

    /**
     * Returns the bootstrap controllers string.
     */
    public String bootstrapControllers() {
        return containers.entrySet().stream()
            .filter(e -> getProcessRoles(e.getKey()).contains("controller"))
            .filter(e -> runningNodeIds.contains(e.getKey()))
            .map(e -> "localhost:" + e.getValue().getMappedPort(CONTROLLER_PORT))
            .collect(Collectors.joining(","));
    }

    /**
     * Returns the cluster ID.
     */
    public String clusterId() {
        return "test-container-cluster-id-01";
    }

    /**
     * Returns the set of broker node IDs.
     */
    public Set<Integer> brokerIds() {
        return containers.keySet().stream()
            .filter(id -> getProcessRoles(id).contains("broker"))
            .collect(Collectors.toSet());
    }

    /**
     * Returns the set of controller node IDs.
     */
    public Set<Integer> controllerIds() {
        return containers.keySet().stream()
            .filter(id -> getProcessRoles(id).contains("controller"))
            .collect(Collectors.toSet());
    }

    /**
     * Returns the underlying container map for inspection.
     */
    public Map<Integer, GenericContainer<?>> containers() {
        return Collections.unmodifiableMap(containers);
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * A Kafka container node that injects the correct advertised listeners
     * (with Docker-mapped host port) via a startup script before Kafka starts.
     */
    private static class KafkaNode extends GenericContainer<KafkaNode> {
        private final int nodeId;
        private final String processRoles;

        KafkaNode(DockerImageName image, int nodeId, String processRoles) {
            super(image);
            this.nodeId = nodeId;
            this.processRoles = processRoles;
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
            if (processRoles.contains("broker")) {
                int mappedPort = this.getMappedPort(KAFKA_PORT);
                script.append("export KAFKA_ADVERTISED_LISTENERS='EXTERNAL://localhost:")
                    .append(mappedPort)
                    .append(",INTERNAL://kafka-").append(nodeId).append(":").append(INTERNAL_PORT)
                    .append("'\n");
            }
            script.append("exec /etc/kafka/docker/run\n");
            copyFileToContainer(
                Transferable.of(script.toString().getBytes(StandardCharsets.UTF_8), 0777),
                STARTER_SCRIPT);
        }
    }
}
