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
import java.util.function.Predicate;

/**
 * Defines Kafka release versions available as Docker images for container-based testing.
 * Each constant maps to a Docker image tag on {@code apache/kafka:<version>}.
 *
 * <p>This is the Java equivalent of the Python {@code tests/kafkatest/version.py}.
 * Docker images for Apache Kafka are available from 3.7.0 onward.
 */
public final class KafkaVersions {

    private KafkaVersions() {}

    private static final String IMAGE_PREFIX = "apache/kafka:";

    // 3.7.x
    public static final String LATEST_3_7 = "3.7.2";

    // 3.8.x
    public static final String LATEST_3_8 = "3.8.1";

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
        LATEST_3_7,
        LATEST_3_8,
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
        return filterImages(v -> true);
    }

    /**
     * Returns container image names for versions matching the given major version.
     * For example, {@code crossVersionImagesByMajor(4)} returns images for 4.0.x, 4.1.x, 4.2.x.
     */
    public static String[] crossVersionImagesByMajor(int major) {
        return filterImages(v -> parseMajor(v) == major);
    }

    /**
     * Returns container image names for versions matching the given major.minor.
     * For example, {@code crossVersionImagesByMinor(3, 9)} returns images for 3.9.x only.
     */
    public static String[] crossVersionImagesByMinor(int major, int minor) {
        return filterImages(v -> parseMajor(v) == major && parseMinor(v) == minor);
    }

    /**
     * Returns container image names for versions at or above the given version string.
     * For example, {@code crossVersionImagesFrom("4.0.0")} returns 4.0.x, 4.1.x, 4.2.x.
     */
    public static String[] crossVersionImagesFrom(String minVersion) {
        return filterImages(v -> compareVersions(v, minVersion) >= 0);
    }

    /**
     * Returns container image names matching an arbitrary predicate on the version string.
     */
    public static String[] filterImages(Predicate<String> filter) {
        return CROSS_VERSION_TEST_VERSIONS.stream()
            .filter(filter)
            .map(KafkaVersions::toImageName)
            .toArray(String[]::new);
    }

    /**
     * Returns the full Docker image name for a given Kafka version string.
     * For example, {@code toImageName("4.2.0")} returns {@code "apache/kafka:4.2.0"}.
     */
    public static String toImageName(String version) {
        return IMAGE_PREFIX + version;
    }

    private static int parseMajor(String version) {
        return Integer.parseInt(version.split("\\.")[0]);
    }

    private static int parseMinor(String version) {
        return Integer.parseInt(version.split("\\.")[1]);
    }

    private static int parsePatch(String version) {
        return Integer.parseInt(version.split("\\.")[2]);
    }

    private static int compareVersions(String a, String b) {
        int cmp = Integer.compare(parseMajor(a), parseMajor(b));
        if (cmp != 0) return cmp;
        cmp = Integer.compare(parseMinor(a), parseMinor(b));
        if (cmp != 0) return cmp;
        return Integer.compare(parsePatch(a), parsePatch(b));
    }
}