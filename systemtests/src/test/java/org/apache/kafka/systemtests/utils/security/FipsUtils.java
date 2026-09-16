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

package org.apache.kafka.systemtests.utils.security;

import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.systemtests.utils.cli.ContainerCommandUtils;

import org.testcontainers.containers.Container;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Detects a genuinely FIPS-enabled kernel rather than simulating one. Linux containers share the
 * host kernel, so {@value #PROC_FIPS_ENABLED} reads the same inside a container as on the host with
 * no bind-mount trickery: a container on a FIPS-mode host sees {@code 1} natively.
 */
public final class FipsUtils {

    public static final String PROC_FIPS_ENABLED = "/proc/sys/crypto/fips_enabled";

    private FipsUtils() {
    }

    /**
     * True only when the kernel this JVM runs on is in FIPS mode. That is the container runtime's
     * kernel too whenever the runtime runs directly on this host; behind a macOS/Windows VM the JVM
     * cannot see the container kernel at all, and this correctly reports false.
     */
    public static boolean isHostFipsEnabled() {
        try {
            return Files.readString(Path.of(PROC_FIPS_ENABLED)).trim().equals("1");
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Asserts that the broker under test is itself running under a FIPS kernel, which is the
     * property the FIPS tests actually rely on. {@link #isHostFipsEnabled()} checks the test JVM's
     * kernel and the two only differ when the container runtime is remote, so this is cheap
     * insurance that a green FIPS test was really exercising a FIPS broker.
     */
    public static void assertBrokerInFipsMode(ClusterInstance cluster) throws Exception {
        Container.ExecResult result = ContainerCommandUtils.runCommand(cluster, "cat", PROC_FIPS_ENABLED);
        assertEquals(0, result.getExitCode(), "Could not read " + PROC_FIPS_ENABLED + " in the broker container: " + result.getStderr());
        assertEquals("1", result.getStdout().trim(), "The broker container is not running under a FIPS kernel");
    }
}
