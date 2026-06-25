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

import org.apache.kafka.common.test.api.ClusterConfig;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * JUnit 5 {@link TestTemplateInvocationContext} for container-based Kafka clusters.
 * Manages the lifecycle of a {@link ContainerClusterInstance} for each test invocation.
 *
 * <p>This context mirrors the structure of {@link RaftClusterInvocationContext} but uses
 * Docker containers instead of in-process servers. The cluster is started before each test
 * and stopped after test execution.
 */
public class ContainerClusterInvocationContext implements TestTemplateInvocationContext {

    private final String baseDisplayName;
    private final ClusterConfig clusterConfig;
    private final boolean isCombined;

    public ContainerClusterInvocationContext(String baseDisplayName, ClusterConfig clusterConfig, boolean isCombined) {
        this.baseDisplayName = baseDisplayName;
        this.clusterConfig = clusterConfig;
        this.isCombined = isCombined;
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        return String.format("%s [%d] Type=Container-%s, %s", baseDisplayName, invocationIndex,
            isCombined ? "Combined" : "Isolated",
            String.join(",", clusterConfig.displayTags()));
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        ContainerClusterInstance clusterInstance = new ContainerClusterInstance(clusterConfig, isCombined);
        return List.of(
            (BeforeEachCallback) context -> {
                if (clusterConfig.isAutoStart()) {
                    try {
                        clusterInstance.start();
                        clusterInstance.waitForReadyBrokers();
                    } catch (Exception e) {
                        clusterInstance.setLogDir(buildLogDir(context));
                        throw e;
                    }
                }
            },
            (TestExecutionExceptionHandler) (context, throwable) -> {
                clusterInstance.setLogDir(buildLogDir(context));
                throw throwable;
            },
            (AfterTestExecutionCallback) context -> {
                if (context.getExecutionException().isPresent()) {
                    clusterInstance.setLogDir(buildLogDir(context));
                }
            },
            (AfterEachCallback) context -> clusterInstance.stop(),
            new ClusterInstanceParameterResolver(clusterInstance)
        );
    }

    private static final String SESSION_TIMESTAMP =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));

    private Path buildLogDir(ExtensionContext context) {
        String className = context.getRequiredTestClass().getSimpleName();
        String methodName = context.getRequiredTestMethod().getName();
        String displayName = context.getDisplayName()
            .replaceAll("[^a-zA-Z0-9._-]", "_");
        return Path.of("build", "test-logs", SESSION_TIMESTAMP, className, methodName, displayName);
    }
}
