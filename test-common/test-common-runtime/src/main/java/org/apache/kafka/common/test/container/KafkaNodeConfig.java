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

    private KafkaNodeConfig(Builder builder) {
        this.securityProtocol = builder.securityProtocol;
        this.saslMechanism = builder.saslMechanism;
        this.serverProperties = Collections.unmodifiableMap(new HashMap<>(builder.serverProperties));
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

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        private String saslMechanism = null;
        private Map<String, String> serverProperties = new HashMap<>();

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

        public KafkaNodeConfig build() {
            return new KafkaNodeConfig(this);
        }
    }
}
