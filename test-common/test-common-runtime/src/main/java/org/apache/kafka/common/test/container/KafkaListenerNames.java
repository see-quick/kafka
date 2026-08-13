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
 * Listener name constants used in Kafka container cluster configuration.
 * <ul>
 *   <li>{@code EXTERNAL} — client-facing listener, accessible from the Docker host</li>
 *   <li>{@code INTERNAL} — inter-broker listener, accessible within the Docker network</li>
 *   <li>{@code CONTROLLER} — KRaft controller listener, accessible within the Docker network</li>
 * </ul>
 */
final class KafkaListenerNames {

    static final String EXTERNAL = "EXTERNAL";
    static final String INTERNAL = "INTERNAL";
    static final String CONTROLLER = "CONTROLLER";

    private KafkaListenerNames() {
    }
}