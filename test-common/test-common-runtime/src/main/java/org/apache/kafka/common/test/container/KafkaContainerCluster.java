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

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.JaasUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
    static final String STARTER_SCRIPT = "/tmp/start_kafka.sh";
    static final int KAFKA_PORT = 9092;
    private static final int CONTROLLER_PORT = 9093;
    static final int INTERNAL_PORT = 9094;
    static final String CLUSTER_ID = "test-container-cluster-id-01";
    static final String DNS_PREFIX = "kafka-";

    private final int numBrokers;
    private final int numControllers;
    private final boolean combined;
    private final KafkaNodeConfig nodeConfig;
    private final Network network;
    private final Map<Integer, GenericContainer<?>> containers;
    // Tracks which nodes are running internally rather than querying the Docker API
    // via container.isRunning(). After a Docker stop/start cycle the Docker daemon
    // can become temporarily unresponsive (NoHttpResponseException on localhost:2375),
    // which would cause bootstrapServers()/bootstrapControllers() to incorrectly
    // exclude brokers that are actually running.
    private final Set<Integer> runningNodeIds;
    private final DockerImageName imageName;
    private Path logDir;

    @SuppressWarnings("resource")
    public KafkaContainerCluster(int numBrokers, int numControllers, boolean combined,
                                  KafkaNodeConfig nodeConfig,
                                  String containerImage) {
        this.numBrokers = numBrokers;
        this.numControllers = numControllers;
        this.combined = combined;
        this.nodeConfig = nodeConfig;
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
            KafkaNodeRole role = getNodeRole(nodeId);
            Map<String, String> config = buildNodeConfig(nodeId, role, quorumVoters);

            KafkaNode container = new KafkaNode(imageName, nodeId, role);

            container
                .withNetwork(network)
                .withNetworkAliases(DNS_PREFIX + nodeId)
                .withEnv(Collections.unmodifiableMap(config));

            if (role.isBroker() && role.isController()) {
                container.withExposedPorts(KAFKA_PORT, CONTROLLER_PORT);
            } else if (role.isBroker()) {
                container.withExposedPorts(KAFKA_PORT);
            } else {
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

    private KafkaNodeRole getNodeRole(int nodeId) {
        if (combined) {
            boolean isBroker = nodeId < numBrokers;
            boolean isController = nodeId < numControllers;
            if (isBroker && isController) return KafkaNodeRole.COMBINED;
            if (isBroker) return KafkaNodeRole.BROKER;
            return KafkaNodeRole.CONTROLLER;
        } else {
            if (nodeId < numControllers) return KafkaNodeRole.CONTROLLER;
            return KafkaNodeRole.BROKER;
        }
    }

    private String buildQuorumVoters() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numControllers; i++) {
            if (!sb.isEmpty()) sb.append(",");
            sb.append(i).append("@").append(DNS_PREFIX).append(i).append(":").append(CONTROLLER_PORT);
        }
        return sb.toString();
    }

    private Map<String, String> buildNodeConfig(int nodeId, KafkaNodeRole role, String quorumVoters) {
        Map<String, String> config = new HashMap<>();

        config.put(KafkaEnvVars.NODE_ID, String.valueOf(nodeId));
        config.put(KafkaEnvVars.PROCESS_ROLES, role.getProcessRoles());
        config.put(KafkaEnvVars.CONTROLLER_QUORUM_VOTERS, quorumVoters);
        config.put(KafkaEnvVars.CLUSTER_ID, CLUSTER_ID);

        if (role.isBroker()) {
            config.put(KafkaEnvVars.LISTENERS,
                Listener.EXTERNAL + "://0.0.0.0:" + KAFKA_PORT +
                "," + Listener.INTERNAL + "://0.0.0.0:" + INTERNAL_PORT +
                (role.isController() ? "," + Listener.CONTROLLER + "://0.0.0.0:" + CONTROLLER_PORT : ""));

            // KAFKA_ADVERTISED_LISTENERS is NOT set here — it is injected by the
            // containerIsStarting() hook with the correct Docker-mapped host port.

            config.put(KafkaEnvVars.LISTENER_SECURITY_PROTOCOL_MAP,
                Listener.EXTERNAL + ":" + nodeConfig.securityProtocol().name() +
                "," + Listener.INTERNAL + ":" + Listener.PLAINTEXT +
                "," + Listener.CONTROLLER + ":" + Listener.PLAINTEXT);
            config.put(KafkaEnvVars.INTER_BROKER_LISTENER_NAME, Listener.INTERNAL);
        } else {
            config.put(KafkaEnvVars.LISTENERS, Listener.CONTROLLER + "://0.0.0.0:" + CONTROLLER_PORT);
            config.put(KafkaEnvVars.LISTENER_SECURITY_PROTOCOL_MAP, Listener.CONTROLLER + ":" + Listener.PLAINTEXT);
        }

        config.put(KafkaEnvVars.CONTROLLER_LISTENER_NAMES, Listener.CONTROLLER);
        config.put(KafkaEnvVars.LOG_DIRS, "/var/lib/kafka/data");
        config.put(KafkaEnvVars.OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(Math.min(numBrokers, 3)));
        config.put(KafkaEnvVars.GROUP_INITIAL_REBALANCE_DELAY_MS, "0");
        config.put(KafkaEnvVars.TRANSACTION_STATE_LOG_MIN_ISR, "1");
        config.put(KafkaEnvVars.TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(Math.min(numBrokers, 3)));

        if (role.isBroker() && isSaslProtocol()) {
            String saslMechanism = nodeConfig.saslMechanism();
            config.put(KafkaEnvVars.SASL_ENABLED_MECHANISMS, saslMechanism);
            if ("PLAIN".equals(saslMechanism)) {
                String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + JaasUtils.KAFKA_PLAIN_ADMIN + "\" "
                    + "password=\"" + JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD + "\" "
                    + "user_" + JaasUtils.KAFKA_PLAIN_ADMIN + "=\"" + JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD + "\" "
                    + "user_" + JaasUtils.KAFKA_PLAIN_USER1 + "=\"" + JaasUtils.KAFKA_PLAIN_USER1_PASSWORD + "\";";
                config.put("KAFKA_LISTENER_NAME_" + Listener.EXTERNAL + "_PLAIN_SASL_JAAS_CONFIG", jaasConfig);
            } else if (saslMechanism != null && saslMechanism.startsWith("SCRAM-")) {
                String envMechanism = saslMechanism.replace("-", "__");
                String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required;";
                config.put("KAFKA_LISTENER_NAME_" + Listener.EXTERNAL + "_" + envMechanism + "_SASL_JAAS_CONFIG", jaasConfig);
            }
        }

        for (Map.Entry<String, String> entry : nodeConfig.serverProperties().entrySet()) {
            config.put("KAFKA_" + entry.getKey().replace(".", "_").toUpperCase(Locale.ROOT), entry.getValue());
        }

        return config;
    }

    private boolean isSaslProtocol() {
        return nodeConfig.securityProtocol() == SecurityProtocol.SASL_PLAINTEXT
            || nodeConfig.securityProtocol() == SecurityProtocol.SASL_SSL;
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
            numBrokers, numControllers, combined, nodeConfig.securityProtocol());

        Exception lastException = null;
        for (int attempt = 1; attempt <= ContainerUtils.MAX_DOCKER_RETRIES; attempt++) {
            try {
                Startables.deepStart(containers.values().stream()).get(120, TimeUnit.SECONDS);
                lastException = null;
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while starting Kafka containers", e);
            } catch (ExecutionException | TimeoutException e) {
                lastException = e;
                if (attempt < ContainerUtils.MAX_DOCKER_RETRIES) {
                    long backoff = ContainerUtils.DOCKER_RETRY_INITIAL_BACKOFF_MS * (1L << (attempt - 1));
                    LOG.warn("Failed to start Kafka containers (attempt {}/{}), retrying in {}ms",
                        attempt, ContainerUtils.MAX_DOCKER_RETRIES, backoff, e);
                    recreateContainers();
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while retrying container startup", ie);
                    }
                }
            }
        }
        if (lastException != null) {
            ContainerUtils.logDockerDaemonDiagnostics();
            throw new RuntimeException("Failed to start Kafka containers after " + ContainerUtils.MAX_DOCKER_RETRIES + " attempts", lastException);
        }

        runningNodeIds.addAll(containers.keySet());

        if (isSaslProtocol() && nodeConfig.saslMechanism() != null && nodeConfig.saslMechanism().startsWith("SCRAM-")) {
            createScramUsers();
        }

        LOG.info("Kafka container cluster started. Bootstrap servers: {}", bootstrapServers());
    }

    private void createScramUsers() {
        GenericContainer<?> brokerContainer = containers.entrySet().stream()
            .filter(e -> getNodeRole(e.getKey()).isBroker())
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No broker container found"));

        createScramUser(brokerContainer, JaasUtils.KAFKA_PLAIN_ADMIN, JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD);
        createScramUser(brokerContainer, JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD);
    }

    private void createScramUser(GenericContainer<?> container, String username, String password) {
        try {
            org.testcontainers.containers.Container.ExecResult result = container.execInContainer(
                "/opt/kafka/bin/kafka-configs.sh",
                "--bootstrap-server", "localhost:" + INTERNAL_PORT,
                "--alter",
                "--add-config", nodeConfig.saslMechanism() + "=[password=" + password + ",iterations=4096]",
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
     * Sets the directory where container logs will be collected on stop.
     */
    public void setLogDir(Path logDir) {
        this.logDir = logDir;
    }

    /**
     * Stops all containers in the cluster, collecting logs beforehand if a log directory is set.
     *
     * <p>The Docker network is intentionally NOT closed here. Testcontainers'
     * {@code ResourceReaper} JVM shutdown hook handles both container removal and
     * network cleanup in the correct order (containers first, then networks).
     * Closing the network eagerly causes the reaper to fail with "network not found"
     * when it later tries to remove containers still referencing that network.
     */
    public void stop() {
        LOG.info("Stopping Kafka container cluster");
        collectLogs();
        runningNodeIds.clear();
        for (GenericContainer<?> container : containers.values()) {
            try {
                container.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping container", e);
            }
        }
    }

    private void collectLogs() {
        if (logDir == null) {
            return;
        }
        try {
            Files.createDirectories(logDir);
        } catch (IOException e) {
            LOG.warn("Failed to create log directory: {}", logDir, e);
            return;
        }
        for (Map.Entry<Integer, GenericContainer<?>> entry : containers.entrySet()) {
            int nodeId = entry.getKey();
            GenericContainer<?> container = entry.getValue();
            KafkaNodeRole nodeRole = getNodeRole(nodeId);
            String fileName;
            if (nodeRole == KafkaNodeRole.COMBINED) {
                fileName = "kafka-combined-" + nodeId + ".log";
            } else if (nodeRole.isBroker()) {
                fileName = "kafka-broker-" + nodeId + ".log";
            } else {
                fileName = "kafka-controller-" + nodeId + ".log";
            }
            try {
                String logs = container.getLogs();
                Files.writeString(logDir.resolve(fileName), logs);
                LOG.info("Collected logs for node {} to {}", nodeId, logDir.resolve(fileName));
            } catch (Exception e) {
                LOG.warn("Failed to collect logs for node {}", nodeId, e);
            }
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
        ContainerUtils.retryOnDockerFailure(() ->
            DockerClientFactory.lazyClient().stopContainerCmd(container.getContainerId()).exec(),
            "stop broker " + nodeId);
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
        ContainerUtils.retryOnDockerFailure(() ->
            DockerClientFactory.lazyClient().startContainerCmd(container.getContainerId()).exec(),
            "start broker " + nodeId);
        runningNodeIds.add(nodeId);
    }

    private void recreateContainers() {
        for (GenericContainer<?> container : containers.values()) {
            try {
                container.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping container during retry cleanup", e);
            }
        }
        containers.clear();
        createContainers();
    }

    /**
     * Returns the bootstrap servers string for client connections.
     */
    public String bootstrapServers() {
        return containers.entrySet().stream()
            .filter(e -> getNodeRole(e.getKey()).isBroker())
            .filter(e -> runningNodeIds.contains(e.getKey()))
            .map(e -> "localhost:" + e.getValue().getMappedPort(KAFKA_PORT))
            .collect(Collectors.joining(","));
    }

    /**
     * Returns the bootstrap controllers string.
     */
    public String bootstrapControllers() {
        return containers.entrySet().stream()
            .filter(e -> getNodeRole(e.getKey()).isController())
            .filter(e -> runningNodeIds.contains(e.getKey()))
            .map(e -> "localhost:" + e.getValue().getMappedPort(CONTROLLER_PORT))
            .collect(Collectors.joining(","));
    }

    /**
     * Returns the cluster ID.
     */
    public String clusterId() {
        return CLUSTER_ID;
    }

    /**
     * Returns the set of broker node IDs.
     */
    public Set<Integer> brokerIds() {
        return containers.keySet().stream()
            .filter(id -> getNodeRole(id).isBroker())
            .collect(Collectors.toSet());
    }

    /**
     * Returns the set of controller node IDs.
     */
    public Set<Integer> controllerIds() {
        return containers.keySet().stream()
            .filter(id -> getNodeRole(id).isController())
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
}
