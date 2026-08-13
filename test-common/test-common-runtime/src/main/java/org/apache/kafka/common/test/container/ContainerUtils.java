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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;

import java.util.concurrent.TimeUnit;

public final class ContainerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ContainerUtils.class);

    static final int MAX_DOCKER_RETRIES = 5;
    static final long DOCKER_RETRY_INITIAL_BACKOFF_MS = 1000;

    private ContainerUtils() {
    }

    static void retryOnDockerFailure(Runnable action, String description) {
        for (int attempt = 1; attempt <= MAX_DOCKER_RETRIES; attempt++) {
            try {
                action.run();
                return;
            } catch (Exception e) {
                if (attempt == MAX_DOCKER_RETRIES) {
                    logDockerDaemonDiagnostics();
                    throw new RuntimeException("Failed to " + description + " after " + MAX_DOCKER_RETRIES + " attempts", e);
                }
                long backoff = DOCKER_RETRY_INITIAL_BACKOFF_MS * (1L << (attempt - 1));
                LOG.warn("Docker command '{}' failed (attempt {}/{}), retrying in {}ms",
                    description, attempt, MAX_DOCKER_RETRIES, backoff, e);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying " + description, ie);
                }
            }
        }
    }

    static void logDockerDaemonDiagnostics() {
        try {
            var client = DockerClientFactory.lazyClient();
            var info = client.infoCmd().exec();
            LOG.error("Docker daemon info: containers={}, containersRunning={}, containersPaused={}, containersStopped={}, " +
                    "images={}, memoryTotal={}MB, cpus={}, serverVersion={}, operatingSystem={}",
                info.getContainers(), info.getContainersRunning(), info.getContainersPaused(),
                info.getContainersStopped(), info.getImages(),
                info.getMemTotal() != null ? info.getMemTotal() / (1024 * 1024) : "unknown",
                info.getNCPU(), info.getServerVersion(), info.getOperatingSystem());
        } catch (Exception e) {
            LOG.error("Docker daemon is unreachable, cannot collect diagnostics", e);
        }
        collectDaemonLogs();
    }

    private static void collectDaemonLogs() {
        String binary = resolveContainerBinary();
        String[][] commands = {
            {binary, "events", "--since=60s", "--until=0s", "--format", "{{.Time}} {{.Type}} {{.Action}} {{.Actor.Attributes.name}}"},
            {binary, "system", "df"},
        };
        for (String[] cmd : commands) {
            try {
                Process process = new ProcessBuilder(cmd)
                    .redirectErrorStream(true)
                    .start();
                boolean finished = process.waitFor(10, TimeUnit.SECONDS);
                String output = new String(process.getInputStream().readAllBytes());
                if (!finished) {
                    process.destroyForcibly();
                    LOG.error("'{}' timed out after 10s, partial output:\n{}", String.join(" ", cmd), output);
                } else if (!output.isBlank()) {
                    LOG.error("'{}' output:\n{}", String.join(" ", cmd), output);
                }
            } catch (Exception e) {
                LOG.warn("Failed to run '{}': {}", String.join(" ", cmd), e.getMessage());
            }
        }
    }

    static String resolveContainerBinary() {
        for (String candidate : new String[]{"podman", "docker"}) {
            try {
                Process p = new ProcessBuilder("which", candidate)
                    .redirectErrorStream(true)
                    .start();
                if (p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0) {
                    return candidate;
                }
            } catch (Exception ignored) {
            }
        }
        return "docker";
    }
}