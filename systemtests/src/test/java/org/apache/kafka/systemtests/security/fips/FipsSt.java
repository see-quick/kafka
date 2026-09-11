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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterSystemTemplate;
import org.apache.kafka.systemtests.utils.ClientUtils;
import org.apache.kafka.systemtests.utils.security.TlsCluster;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The UBI10 image running on a genuinely FIPS-mode host, where the container transparently
 * inherits the host kernel's real {@code /proc/sys/crypto/fips_enabled=1} and Red Hat's FIPS
 * provider swap activates for real — no bind-mount simulation. Gated by
 * {@link FipsExecutionCondition}, which skips these tests on any host that isn't actually running
 * in FIPS mode (that includes every macOS/Windows Docker Desktop or Podman machine setup, since
 * the container runtime there runs inside a non-FIPS Linux VM regardless of the host OS).
 *
 * <p>Requires a UBI image built beforehand, e.g.
 * {@code ./gradlew :systemtests:buildSystemTestImage -PsystemTestImage=kafka-systemtest:ubi-local -PsystemTestImageType=jvm-ubi}.
 */
@ExtendWith(FipsExecutionCondition.class)
public class FipsSt {

    private static final int NUM_MESSAGES = 100;

    static List<ClusterConfig> generateFipsSslConfigs() throws Exception {
        return List.of(TlsCluster.builder()
            .containerImage(FipsFixture.imageTag())
            .build());
    }

    /**
     * A broker whose keystore and truststore are PEM, under FIPS. Unlike the PKCS12 configuration
     * above, this is the one that actually exercises KAFKA-20997: {@code PemStore}'s static
     * {@code KEY_FACTORIES} field eagerly builds an RSA, a DSA and an EC {@code KeyFactory}, and
     * the DSA one is unavailable under Red Hat's FIPS providers, so the class initializer fails
     * and every PEM store breaks — RSA-only ones included. A PKCS12 broker never loads the class,
     * which is why {@link #generateFipsSslConfigs()} passes with or without the fix.
     *
     * <p>The client deliberately uses a PKCS12 truststore rather than PEM, so a failure is
     * unambiguously the broker's PEM path inside the FIPS container rather than the test JVM's own
     * client-side PEM parsing (whose FIPS state depends on which JDK build runs the tests).
     */
    static List<ClusterConfig> generateFipsPemConfigs() throws Exception {
        return List.of(TlsCluster.builder()
            .brokerStoreType(TlsCluster.StoreType.PEM)
            .clientTrustStoreType(TlsCluster.StoreType.PKCS12)
            .containerImage(FipsFixture.imageTag())
            .build());
    }

    /**
     * A PKCS12 TLS cluster booting and serving traffic on a genuinely FIPS-mode host. PKCS12
     * keystores remain available under Red Hat's FIPS providers, so this is a baseline that the
     * common TLS path still works — it does <em>not</em> cover KAFKA-20997, which only fires for
     * PEM stores; see {@link #testProduceConsumeOverPemTlsUnderFips(ClusterInstance)} for that.
     */
    @Tag("fips")
    @Timeout(180)
    @ClusterSystemTemplate("generateFipsSslConfigs")
    void testProduceConsumeAndAdminOverTlsUnderFips(ClusterInstance cluster) throws Exception {
        String topicName = "fips-tls-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        try (Admin admin = cluster.admin()) {
            assertFalse(admin.describeCluster().nodes().get().isEmpty(), "Should describe cluster over TLS under FIPS");
        }

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }

    /**
     * The test that would have caught KAFKA-20997 at the system level: a broker with PEM
     * keystore/truststore booting and serving traffic under FIPS. Without the fix, the broker
     * fails to start because {@code PemStore}'s class initializer cannot build a DSA
     * {@code KeyFactory}, so this fails at cluster startup rather than at produce/consume.
     */
    @Tag("fips")
    @Timeout(180)
    @ClusterSystemTemplate("generateFipsPemConfigs")
    void testProduceConsumeOverPemTlsUnderFips(ClusterInstance cluster) throws Exception {
        String topicName = "fips-pem-tls-test-topic";
        cluster.createTopic(topicName, 1, (short) 1);

        try (Admin admin = cluster.admin()) {
            assertFalse(admin.describeCluster().nodes().get().isEmpty(),
                "Should describe cluster over PEM TLS under FIPS");
        }

        ClientUtils.produceMessages(cluster, topicName, NUM_MESSAGES);
        List<ConsumerRecord<String, String>> records = ClientUtils.consumeMessages(
            cluster, topicName, 1, NUM_MESSAGES, 30_000L);

        assertEquals(NUM_MESSAGES, records.size());
    }

    /**
     * Pins the exact KAFKA-20997 regression — DSA KeyFactory unavailable, RSA/SHA-256 still fine
     * — without booting a broker. Runs {@link FipsDsaProbe} directly inside a container on a
     * genuinely FIPS-mode host, where the container inherits the real host kernel's FIPS state
     * with no bind-mount needed.
     */
    @Tag("system")
    @Tag("fips")
    @Test
    @Timeout(60)
    @SuppressWarnings("resource")
    void dsaKeyFactoryUnavailableWhileRsaAndSha256StillWork() throws Exception {
        String probeClassName = FipsDsaProbe.class.getName();
        String probeRelativePath = probeClassName.replace('.', '/') + ".class";
        byte[] probeBytes;
        try (InputStream in = FipsDsaProbe.class.getResourceAsStream("FipsDsaProbe.class")) {
            assertTrue(in != null, "Could not locate compiled " + probeClassName);
            probeBytes = in.readAllBytes();
        }

        try (GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(FipsFixture.imageTag()))) {
            container.withCommand("sleep", "60");
            container.start();
            container.copyFileToContainer(Transferable.of(probeBytes), "/tmp/probe/" + probeRelativePath);

            Container.ExecResult result = container.execInContainer("java", "-cp", "/tmp/probe", probeClassName);
            assertEquals(0, result.getExitCode(), "Probe should run cleanly: " + result.getStderr());

            String stdout = result.getStdout();
            assertTrue(stdout.contains("DSA_KEYFACTORY=UNAVAILABLE"),
                "Expected DSA KeyFactory to be unavailable under FIPS, got: " + stdout);
            assertTrue(stdout.contains("RSA_KEYFACTORY=AVAILABLE"),
                "Expected RSA KeyFactory to still work under FIPS, got: " + stdout);
            assertTrue(stdout.contains("SHA256_DIGEST=AVAILABLE"),
                "Expected SHA-256 digest to still work under FIPS, got: " + stdout);
        }
    }
}
