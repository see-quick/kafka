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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ExecutionMode;
import org.apache.kafka.systemtests.utils.ClientUtils;
import org.apache.kafka.systemtests.utils.security.TlsFixture;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        TlsFixture tls = TlsFixture.generate();
        byte[] serverKeyStore = tls.serverKeyStoreBytes(TlsFixture.STORE_TYPE_PKCS12, "kafka-0", "kafka-1", "localhost");
        byte[] trustStore = tls.trustStoreBytes(TlsFixture.STORE_TYPE_PKCS12);
        String storePassword = new String(TlsFixture.STORE_PASSWORD);

        String keyStorePath = "/etc/kafka/secrets/kafka.server.keystore.p12";
        String trustStorePath = "/etc/kafka/secrets/kafka.server.truststore.p12";

        Map<String, byte[]> filesToMount = Map.of(
            keyStorePath, serverKeyStore,
            trustStorePath, trustStore);

        Map<String, String> extraEnv = new HashMap<>();
        extraEnv.put("KAFKA_SSL_KEYSTORE_LOCATION", keyStorePath);
        extraEnv.put("KAFKA_SSL_KEYSTORE_PASSWORD", storePassword);
        extraEnv.put("KAFKA_SSL_KEYSTORE_TYPE", TlsFixture.STORE_TYPE_PKCS12);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_LOCATION", trustStorePath);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_PASSWORD", storePassword);
        extraEnv.put("KAFKA_SSL_TRUSTSTORE_TYPE", TlsFixture.STORE_TYPE_PKCS12);
        extraEnv.put("KAFKA_SSL_CLIENT_AUTH", "none");

        Map<String, Object> clientSslConfig = Map.of(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM",
            SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, tls.caCertificatePem());

        ClusterConfig config = ClusterConfig.defaultBuilder()
            .setExecutionModes(Set.of(ExecutionMode.CONTAINER))
            .setBrokers(1)
            .setControllers(1)
            .setBrokerSecurityProtocol(SecurityProtocol.SSL)
            .setContainerImage(FipsFixture.imageTag())
            .setExtraEnv(extraEnv)
            .setFilesToMount(filesToMount)
            .setClientSslConfig(clientSslConfig)
            .build();

        return List.of(config);
    }

    /**
     * The test that would have caught KAFKA-20997 at the system level — a PKCS12 TLS cluster
     * booting and serving traffic on a genuinely FIPS-mode host.
     */
    @Tag("system")
    @Tag("fips")
    @Timeout(180)
    @ClusterTemplate("generateFipsSslConfigs")
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
