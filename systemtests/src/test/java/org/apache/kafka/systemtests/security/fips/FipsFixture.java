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

package org.apache.kafka.systemtests.security.fips;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Detects a genuinely FIPS-enabled host rather than simulating one. On Linux, containers share
 * the host kernel, so {@code /proc/sys/crypto/fips_enabled} inside a container reflects the real
 * kernel FIPS state with no bind-mount trickery needed — a container on a real FIPS-mode RHEL
 * host sees {@code fips_enabled=1} natively. This class checks that same file as seen by the test
 * JVM itself, which is accurate whenever the JVM and the container runtime share a kernel (a
 * FIPS-mode CI/build host running Docker/Podman directly, not a macOS/Windows VM-backed setup).
 *
 * <p>On a non-FIPS host (including any macOS/Windows Docker Desktop or Podman machine VM, where
 * the JVM's view of {@code /proc} isn't even the container runtime's kernel), this correctly
 * reports disabled and the FIPS tests are skipped — there is no way to fake real FIPS mode, and
 * this class intentionally no longer tries to.
 */
public final class FipsFixture {

    private static final Path PROC_FIPS_ENABLED_PATH = Path.of("/proc/sys/crypto/fips_enabled");

    private FipsFixture() {
    }

    /** True only when this host's kernel is genuinely running in FIPS mode. */
    public static boolean isHostFipsEnabled() {
        try {
            return Files.readString(PROC_FIPS_ENABLED_PATH).trim().equals("1");
        } catch (Exception e) {
            return false;
        }
    }
}
