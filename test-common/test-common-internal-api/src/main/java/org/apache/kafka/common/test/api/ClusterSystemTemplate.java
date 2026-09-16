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

package org.apache.kafka.common.test.api;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * The generator-backed counterpart to {@link ClusterSystemTest}: behaves exactly like
 * {@link ClusterTemplate}, but carries the {@code system} tag and the longer timeout that
 * container-based tests need, instead of {@link ClusterTemplate}'s {@code integration} tag.
 *
 * <p>Reach for {@link ClusterSystemTest} first: the container runtime provisions its own TLS
 * material and SASL credentials from {@code brokerSecurityProtocol} and the server properties, so
 * security alone is no reason for a generator. Use this only when a cluster configuration is
 * genuinely only known at run time, e.g. images discovered dynamically, or bespoke certificates
 * supplied through {@link ClusterConfig#filesToMount()} and {@link ClusterConfig#clientSslConfig()}
 * in place of the generated ones. Without it such tests would have to hand-write
 * {@code @Tag("system")} and {@code @Timeout} on every method and still end up tagged
 * {@code integration}, which puts them in the wrong task's tag filter.
 *
 * <pre>{@code
 * static List<ClusterConfig> generator() {
 *     return List.of(ClusterConfig.defaultBuilder().build());
 * }
 *
 * @ClusterSystemTemplate("generator")
 * void testSomething(ClusterInstance clusterInstance) {
 *     assertNotNull(clusterInstance.bootstrapServers());
 * }
 * }</pre>
 */
@Documented
@Target({METHOD})
@Retention(RUNTIME)
@TestTemplate
@Timeout(120)
@Tag("system")
public @interface ClusterSystemTemplate {
    /**
     * Specify the static method used for generating cluster configs
     */
    String value();
}
