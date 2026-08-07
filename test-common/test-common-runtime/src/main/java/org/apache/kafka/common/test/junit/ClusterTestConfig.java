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
package org.apache.kafka.common.test.junit;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.api.AutoStart;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterSystemTest;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ExecutionMode;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.MetadataVersion;

/**
 * Unified accessor for fields shared between {@link ClusterTest} and {@link ClusterSystemTest}.
 * This avoids duplicating the annotation-to-config extraction logic in {@link ClusterTestExtensions}.
 */
record ClusterTestConfig(
    Type[] types,
    ExecutionMode executionMode,
    int brokers,
    int controllers,
    int disksPerBroker,
    AutoStart autoStart,
    SecurityProtocol brokerSecurityProtocol,
    String brokerListener,
    SecurityProtocol controllerSecurityProtocol,
    String controllerListener,
    MetadataVersion metadataVersion,
    ClusterConfigProperty[] serverProperties,
    String[] tags,
    ClusterFeature[] features,
    boolean standalone
) {
    static ClusterTestConfig from(ClusterTest annot) {
        return new ClusterTestConfig(
            annot.types(),
            ExecutionMode.IN_MEMORY,
            annot.brokers(),
            annot.controllers(),
            annot.disksPerBroker(),
            annot.autoStart(),
            annot.brokerSecurityProtocol(),
            annot.brokerListener(),
            annot.controllerSecurityProtocol(),
            annot.controllerListener(),
            annot.metadataVersion(),
            annot.serverProperties(),
            annot.tags(),
            annot.features(),
            annot.standalone()
        );
    }

    static ClusterTestConfig from(ClusterSystemTest annot) {
        return new ClusterTestConfig(
            annot.types(),
            ExecutionMode.CONTAINER,
            annot.brokers(),
            annot.controllers(),
            annot.disksPerBroker(),
            annot.autoStart(),
            annot.brokerSecurityProtocol(),
            annot.brokerListener(),
            annot.controllerSecurityProtocol(),
            annot.controllerListener(),
            annot.metadataVersion(),
            annot.serverProperties(),
            annot.tags(),
            annot.features(),
            annot.standalone()
        );
    }
}
