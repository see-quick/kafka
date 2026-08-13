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
 * Environment variable names used to configure Kafka nodes in container clusters.
 */
final class KafkaEnvVars {

    static final String NODE_ID = "KAFKA_NODE_ID";
    static final String PROCESS_ROLES = "KAFKA_PROCESS_ROLES";
    static final String CONTROLLER_QUORUM_VOTERS = "KAFKA_CONTROLLER_QUORUM_VOTERS";
    static final String CLUSTER_ID = "CLUSTER_ID";

    static final String LISTENERS = "KAFKA_LISTENERS";
    static final String ADVERTISED_LISTENERS = "KAFKA_ADVERTISED_LISTENERS";
    static final String LISTENER_SECURITY_PROTOCOL_MAP = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP";
    static final String INTER_BROKER_LISTENER_NAME = "KAFKA_INTER_BROKER_LISTENER_NAME";
    static final String CONTROLLER_LISTENER_NAMES = "KAFKA_CONTROLLER_LISTENER_NAMES";

    static final String LOG_DIRS = "KAFKA_LOG_DIRS";
    static final String OFFSETS_TOPIC_REPLICATION_FACTOR = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR";
    static final String GROUP_INITIAL_REBALANCE_DELAY_MS = "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS";
    static final String TRANSACTION_STATE_LOG_MIN_ISR = "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR";
    static final String TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR";

    static final String SASL_ENABLED_MECHANISMS = "KAFKA_SASL_ENABLED_MECHANISMS";

    private KafkaEnvVars() {
    }
}
