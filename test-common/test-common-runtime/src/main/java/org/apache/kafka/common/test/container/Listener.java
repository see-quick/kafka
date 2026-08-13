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

/**
 * Represents a Kafka listener with its name and role.
 * Defines listener name constants to avoid magic strings across the codebase.
 */
class Listener {

    static final String EXTERNAL = "EXTERNAL";
    static final String INTERNAL = "INTERNAL";
    static final String CONTROLLER = "CONTROLLER";
    static final String PLAINTEXT = "PLAINTEXT";

    /**
     * Defines the role of a Kafka listener.
     */
    enum Role {
        /** Client-facing listener, accessible from the Docker host */
        CLIENT,
        /** Inter-broker communication listener, accessible within the Docker network */
        INTER_BROKER,
        /** KRaft controller listener, accessible within the Docker network */
        CONTROLLER
    }

    private final String name;
    private final Role role;

    Listener(String name, Role role) {
        this.name = name;
        this.role = role;
    }

    public String name() {
        return name;
    }

    public Role role() {
        return role;
    }
}
