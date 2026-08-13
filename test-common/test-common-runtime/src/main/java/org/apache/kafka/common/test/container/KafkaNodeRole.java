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
 * Enum representing the different roles a Kafka node can have in KRaft mode.
 */
enum KafkaNodeRole {

    /** Node that acts as both broker and controller. */
    COMBINED("broker,controller"),

    /** Controller-only node that participates in the metadata quorum. */
    CONTROLLER("controller"),

    /** Broker-only node that handles client requests and stores topic data. */
    BROKER("broker");

    private final String processRoles;

    KafkaNodeRole(String processRoles) {
        this.processRoles = processRoles;
    }

    /** Returns the {@code process.roles} configuration value for this node role. */
    String getProcessRoles() {
        return processRoles;
    }

    boolean isBroker() {
        return this == COMBINED || this == BROKER;
    }

    boolean isController() {
        return this == COMBINED || this == CONTROLLER;
    }
}
