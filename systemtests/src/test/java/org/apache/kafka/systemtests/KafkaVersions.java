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

package org.apache.kafka.systemtests;

import java.util.List;

/**
 * Defines Kafka release versions available as Docker images for container-based testing.
 * Each constant maps to a Docker image tag on {@code apache/kafka:<version>}.
 *
 * <p>This is the Java equivalent of the Python {@code tests/kafkatest/version.py}.
 * Docker images for Apache Kafka are available from 3.7.0 onward, but only KRaft-capable
 * versions (3.9+) are included here since container tests use KRaft exclusively.
 */
public final class KafkaVersions {

    private KafkaVersions() {}

    private static final String IMAGE_PREFIX = "apache/kafka:";

    // 3.9.x
    public static final String LATEST_3_9 = "3.9.2";

    // 4.0.x
    public static final String LATEST_4_0 = "4.0.2";

    // 4.1.x
    public static final String LATEST_4_1 = "4.1.2";

    // 4.2.x
    public static final String LATEST_4_2 = "4.2.0";

    /**
     * Versions to include in cross-version compatibility tests.
     * Add new stable releases here as they are published to Docker Hub.
     */
    public static final List<String> CROSS_VERSION_TEST_VERSIONS = List.of(
        LATEST_3_9,
        LATEST_4_0,
        LATEST_4_1,
        LATEST_4_2
    );

    /**
     * Returns container image names for all cross-version test versions.
     * Can be referenced from {@code @ClusterTest(containerImageSource = "crossVersionImages")}
     * when the test class has a static method delegating to this.
     */
    public static String[] crossVersionImages() {
        return CROSS_VERSION_TEST_VERSIONS.stream()
            .map(KafkaVersions::toImageName)
            .toArray(String[]::new);
    }

    // TODO: maybe adding some filters also to all minors/majors etc.

    /**
     * Returns the full Docker image name for a given Kafka version string.
     * For example, {@code toImageName("4.2.0")} returns {@code "apache/kafka:4.2.0"}.
     */
    public static String toImageName(String version) {
        return IMAGE_PREFIX + version;
    }
}