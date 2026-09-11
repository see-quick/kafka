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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Node-level configuration for a {@link KafkaContainerCluster}.
 * Covers security protocol, SASL mechanism, and additional server properties.
 * Cluster-level concerns (node counts, image, topology) remain on {@link KafkaContainerCluster}.
 */
public final class KafkaNodeConfig {

    private final SecurityProtocol securityProtocol;
    private final String saslMechanism;
    private final Map<String, String> serverProperties;
    private final Map<String, String> extraEnv;
    private final Map<Integer, Map<String, String>> perNodeExtraEnv;
    private final Map<String, byte[]> filesToMount;
    private final Map<Integer, Map<String, byte[]>> perNodeFilesToMount;

    private KafkaNodeConfig(Builder builder) {
        this.securityProtocol = builder.securityProtocol;
        this.saslMechanism = builder.saslMechanism;
        this.serverProperties = Collections.unmodifiableMap(new HashMap<>(builder.serverProperties));
        this.extraEnv = Collections.unmodifiableMap(new HashMap<>(builder.extraEnv));
        this.perNodeExtraEnv = deepCopy(builder.perNodeExtraEnv);
        this.filesToMount = Collections.unmodifiableMap(new HashMap<>(builder.filesToMount));
        this.perNodeFilesToMount = deepCopy(builder.perNodeFilesToMount);
    }

    private static <V> Map<Integer, Map<String, V>> deepCopy(Map<Integer, Map<String, V>> source) {
        Map<Integer, Map<String, V>> copy = new HashMap<>();
        for (Map.Entry<Integer, Map<String, V>> entry : source.entrySet()) {
            copy.put(entry.getKey(), Collections.unmodifiableMap(new HashMap<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(copy);
    }

    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    public String saslMechanism() {
        return saslMechanism;
    }

    public Map<String, String> serverProperties() {
        return serverProperties;
    }

    /**
     * Extra environment variables (e.g. {@code KAFKA_SSL_KEYSTORE_LOCATION}) applied to every
     * node, then overridden per node by {@link #extraEnvForNode(int)}. Applied after the
     * cluster's own computed configuration, so these can override defaults such as the
     * PLAINTEXT-only internal/controller listener protocol map.
     */
    public Map<String, String> extraEnv() {
        return extraEnv;
    }

    /** {@link #extraEnv()} merged with any overrides/additions specific to {@code nodeId}. */
    public Map<String, String> extraEnvForNode(int nodeId) {
        Map<String, String> merged = new HashMap<>(extraEnv);
        merged.putAll(perNodeExtraEnv.getOrDefault(nodeId, Map.of()));
        return merged;
    }

    /**
     * Files to copy into every node's container before it starts, keyed by absolute container
     * path (e.g. under {@code /etc/kafka/secrets/}, which is writable by the image's non-root
     * user). Merged with {@link #filesToMountForNode(int)} for node-specific files such as a
     * per-broker server keystore.
     */
    public Map<String, byte[]> filesToMount() {
        return filesToMount;
    }

    /** {@link #filesToMount()} merged with any files specific to {@code nodeId}. */
    public Map<String, byte[]> filesToMountForNode(int nodeId) {
        Map<String, byte[]> merged = new HashMap<>(filesToMount);
        merged.putAll(perNodeFilesToMount.getOrDefault(nodeId, Map.of()));
        return merged;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        private String saslMechanism = null;
        private Map<String, String> serverProperties = new HashMap<>();
        private Map<String, String> extraEnv = new HashMap<>();
        private Map<Integer, Map<String, String>> perNodeExtraEnv = new HashMap<>();
        private Map<String, byte[]> filesToMount = new HashMap<>();
        private Map<Integer, Map<String, byte[]>> perNodeFilesToMount = new HashMap<>();

        private Builder() {
        }

        public Builder securityProtocol(SecurityProtocol securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder saslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
            return this;
        }

        public Builder serverProperties(Map<String, String> serverProperties) {
            this.serverProperties = new HashMap<>(serverProperties);
            return this;
        }

        public Builder extraEnv(Map<String, String> extraEnv) {
            this.extraEnv = new HashMap<>(extraEnv);
            return this;
        }

        public Builder putExtraEnvForNode(int nodeId, String key, String value) {
            this.perNodeExtraEnv.computeIfAbsent(nodeId, k -> new HashMap<>()).put(key, value);
            return this;
        }

        public Builder filesToMount(Map<String, byte[]> filesToMount) {
            this.filesToMount = new HashMap<>(filesToMount);
            return this;
        }

        public Builder putFileToMountForNode(int nodeId, String containerPath, byte[] content) {
            this.perNodeFilesToMount.computeIfAbsent(nodeId, k -> new HashMap<>()).put(containerPath, content);
            return this;
        }

        public KafkaNodeConfig build() {
            return new KafkaNodeConfig(this);
        }
    }
}
