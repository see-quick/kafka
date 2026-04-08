# Proposal: Migrating Kafka System Tests from Ducktape to Testcontainers

## 1. Summary

This document proposes migrating Apache Kafka's system tests from the current Python/Ducktape framework to a Java/Testcontainers-based approach. The goal is to unify the test stack around Java -- Kafka's primary language -- reduce infrastructure complexity, improve execution speed, and integrate system tests into the existing `@ClusterTest` framework (`ClusterTestExtensions`). A proof-of-concept implementation has been completed, demonstrating that multi-node container-based Kafka clusters can be managed, started, stopped, and tested using the same annotation-driven patterns already familiar to Kafka developers from integration tests.

## 2. Motivation

### 2.1 Language Barrier

The Kafka community is overwhelmingly Java-centric. The existing system tests are written in Python using the Ducktape framework, which creates a barrier for contributors who are not familiar with Python, Ducktape's service abstraction model, or its test runner conventions. Requiring contributors to learn a second language and a niche testing framework in order to write or modify system tests discourages participation and slows development.

### 2.3 Execution Speed

Docker containers start in seconds, compared to minutes for full VMs. A three-node Kafka cluster can be provisioned and running in under 30 seconds using Testcontainers, versus several minutes with Vagrant. This difference compounds across the full test suite and significantly affects the developer feedback loop.

### 2.4 Framework Unification

By implementing container-based execution as an adapter within the existing `@ClusterTest` integration test framework (`ClusterTestExtensions`), newcomers can write system tests using the same familiar annotations and patterns they already use for integration tests. The only difference is the execution mode: `ExecutionMode.CONTAINER` instead of `ExecutionMode.IN_MEMORY`. This eliminates the need to learn a separate framework.

### 2.6 Maintenance Consolidation

Consolidating on a single language and test framework reduces the overall maintenance burden. Bug fixes, framework improvements, and tooling enhancements (IDE support, debugging, profiling) benefit both integration and system tests simultaneously. There is no longer a need to maintain parallel Python and Java test infrastructure. However, this also means that changes to the shared test framework (e.g., `ClusterTestExtensions`, `ClusterConfig`, `ClusterInstance`) could affect both integration and system tests. Contributors modifying the framework would need to verify that system tests are not broken, which may require running them as part of the validation process.

### 2.7 Limitations of Containers vs. VMs

This migration is not without trade-offs. Ducktape can target both VMs and containers, which gives it the ability to test scenarios that require full OS-level isolation -- such as kernel-level networking, disk I/O behavior under specific filesystems, or multi-host deployments across separate machines. A container-only approach using Testcontainers cannot replicate these scenarios. Additionally, containers share the host kernel, so certain classes of faults (e.g., FUSE-based filesystem injection via Kibosh) are harder to simulate. The version coverage is also narrower: Apache Kafka Docker images are available from 3.7.0 onward, and the KRaft-exclusive framework limits testing to 3.9+, meaning ZooKeeper-based versions and versions prior to 3.9 cannot be tested.

That said, the benefits outlined above -- language unification, faster execution, framework reuse, lower infrastructure complexity, and reduced maintenance burden -- outweigh these limitations for the majority of system test scenarios. It is worth noting that the goal is not necessarily to migrate every single Ducktape test -- some tests that rely on VM-level capabilities or pre-3.9 version coverage may remain in Ducktape. The objective is to migrate as many tests as possible to the new framework, covering the vast majority of the test surface, while keeping Ducktape as a fallback for the small number of scenarios that genuinely require it.

## 3. Architecture

### 3.1 Integration with the @ClusterTest Framework

The container execution mode is implemented as a new variant within the existing `ClusterTestExtensions` JUnit 5 extension:

- `ExecutionMode.CONTAINER` has been added alongside `ExecutionMode.IN_MEMORY` in the `ExecutionMode` enum.
- Both modes support `Type.KRAFT` (isolated controller and broker roles) and `Type.CO_KRAFT` (combined mode).
- `ContainerClusterInstance` implements the `ClusterInstance` interface using Docker containers managed by Testcontainers.
- `KafkaContainerCluster` manages multi-node Kafka clusters as Docker containers on a shared Docker network, handling quorum configuration, listener setup, and container lifecycle.
- Tests use the same `@ClusterTest`, `@ClusterTests`, and `@ClusterTestDefaults` annotations they use for integration tests.
- The `containerImages` and `containerImageSource` attributes on `@ClusterTest` enable cross-version testing by specifying one or more Docker image tags.
- The `ClusterConfig.Builder.copy()` method supports Cartesian product fan-out across types, execution modes, and container images.

The wiring in `ClusterTestExtensions` is straightforward:

```java
return switch (executionMode) {
    case IN_MEMORY -> new RaftClusterInvocationContext(baseDisplayName, config, isCombined);
    case CONTAINER -> new ContainerClusterInvocationContext(baseDisplayName, config, isCombined);
};
```

A test that runs against containers looks the same as an integration test, with only the execution mode and annotation attributes differing:

```java
@ClusterTestDefaults(executionModes = {ExecutionMode.CONTAINER})
public class BounceST {

    @ClusterTest(brokers = 3, controllers = 3, types = {Type.KRAFT, Type.CO_KRAFT})
    void testBounce(ClusterInstance cluster) throws InterruptedException {
        cluster.createTopic("bounce-test-topic", 1, (short) 3);
        // ... produce, bounce, consume, assert
    }
}
```

### 3.2 Producers and Consumers

Tests use in-process (JVM) Java clients, which is the same approach Ducktape uses with in-process Python clients. The test JVM creates `KafkaProducer` and `KafkaConsumer` instances that connect to the containerized brokers via Docker-mapped external ports. There is no need for containerized clients -- host-side Java clients provide full programmatic control and direct assertions.

For testing console tools (e.g., `kafka-console-consumer.sh`, `kafka-console-producer.sh`), the `execInContainer()` API can be used to run scripts inside the broker containers.

### 3.3 Cross-Version Testing

The `KafkaVersions` class (analogous to Ducktape's `tests/kafkatest/version.py`) defines available Docker image versions centrally:

```java
public final class KafkaVersions {
    public static final String LATEST_3_9 = "3.9.2";
    public static final String LATEST_4_0 = "4.0.2";
    public static final String LATEST_4_1 = "4.1.2";
    public static final String LATEST_4_2 = "4.2.0";

    public static final List<String> CROSS_VERSION_TEST_VERSIONS = List.of(
        LATEST_3_9, LATEST_4_0, LATEST_4_1, LATEST_4_2
    );

    public static String[] crossVersionImages() {
        return CROSS_VERSION_TEST_VERSIONS.stream()
            .map(KafkaVersions::toImageName)
            .toArray(String[]::new);
    }
}
```

Tests reference this via `@ClusterTest(containerImageSource = "crossVersionImages")`, which dynamically resolves the version list at runtime. This tests the current (dev branch) client JARs against older broker versions.

**Limitation:** Testing older client JARs against newer brokers is not straightforward without separate classloaders or containerized client applications. This is an inherent limitation of the in-process client model.

### 3.4 Module Structure

The implementation is spread across three locations:

| Module | Contents |
|---|---|
| `systemtests/` | All migrated system tests (`BounceST`, `CompressionST`, `CrossVersionST`, `KafkaVersions`) |
| `test-common/test-common-runtime/` | Container framework classes (`KafkaContainerCluster`, `ContainerClusterInstance`, `ContainerClusterInvocationContext`) |
| `test-common/test-common-internal-api/` | Annotations and config classes (`@ClusterTest`, `ExecutionMode`, `ClusterConfig`) |

The `systemtests` module depends on `:clients`, `:test-common:test-common-internal-api`, and `:test-common:test-common-runtime`. Tests are gated behind the `containerTests` Gradle property and run sequentially (`maxParallelForks = 1`) by default.

## 4. PoC Status (Phase 0 -- Completed)

The following components have been implemented and are functional:

- **`KafkaContainerCluster`** -- Multi-node containerized Kafka cluster management with parallel startup via `Startables.deepStart()`. Supports KRaft mode with configurable broker/controller counts, combined and isolated roles, SASL/PLAINTEXT and SASL/SCRAM security protocols, per-node process role assignment, and Docker stop/start for individual brokers (preserving container filesystem across restarts).
- **`ContainerClusterInstance`** -- Full `ClusterInstance` implementation backed by Docker containers. Provides `bootstrapServers()`, `bootstrapControllers()`, `brokerIds()`, `controllerIds()`, `shutdownBroker()`, `startBroker()`, `waitForReadyBrokers()`, and all client factory methods. Methods requiring in-process JVM access to server internals (e.g., `brokers()`, `controllers()`) throw `UnsupportedOperationException` by design.
- **`ContainerClusterInvocationContext`** -- JUnit 5 `TestTemplateInvocationContext` that manages the container cluster lifecycle per test invocation, with `BeforeEachCallback` for startup and `AfterEachCallback` for teardown.
- **`ExecutionMode.CONTAINER`** -- New enum value integrated into `ClusterTestExtensions` dispatch logic.
- **`containerImages` and `containerImageSource`** -- New attributes on `@ClusterTest` for specifying Docker images, enabling cross-version test fan-out.
- **`ClusterConfig.Builder.copy()`** -- Builder method for Cartesian product generation across configurations.
- **`KafkaVersions`** -- Centralized version management for Docker image tags.
- **Three migrated tests:**
  - `BounceST` -- Rolling restart test: produce messages, perform a rolling restart of all brokers, produce more messages, consume and verify all messages are present.
  - `CompressionST` -- Produces and consumes messages using all compression types (snappy, gzip, lz4, zstd, none) across both KRAFT and CO_KRAFT topologies.
  - `CrossVersionST` -- Produces and consumes messages using the current client against broker containers running each released Kafka version (3.9.x through 4.2.x).

### 4.1 Migration Example: CompressionTest

To illustrate what a migrated test looks like in practice, this section compares the existing Ducktape `CompressionTest` with its Testcontainers equivalent `CompressionST`.

**Ducktape version** (`tests/kafkatest/tests/client/compression_test.py`):

```python
class CompressionTest(ProduceConsumeValidateTest):
    COMPRESSION_TYPES = ["snappy", "gzip", "lz4", "zstd", "none"]

    def __init__(self, test_context):
        super(CompressionTest, self).__init__(test_context=test_context)
        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1) if quorum.for_test(test_context) == quorum.zk else None
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk, topics={self.topic: {
            "partitions": 10, "replication-factor": 1}})
        self.num_producers = len(self.COMPRESSION_TYPES)
        self.messages_per_producer = 1000
        self.num_consumers = 1

    @cluster(num_nodes=8)
    @matrix(compression_types=[COMPRESSION_TYPES], metadata_quorum=quorum.all_non_upgrade)
    def test_compressed_topic(self, compression_types, metadata_quorum=quorum.zk):
        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.interbroker_security_protocol = self.kafka.security_protocol
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           compression_types=compression_types)
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic,
                                        consumer_timeout_ms=60000)
        self.kafka.start()
        self.run_produce_consume_validate(lambda: wait_until(
            lambda: self.producer.each_produced_at_least(self.messages_per_producer) == True,
            timeout_sec=120, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time"))
```

**Testcontainers version** (`systemtests/.../client/CompressionST.java`):

```java
@ClusterTestDefaults(executionModes = {ExecutionMode.CONTAINER})
public class CompressionST {
    private static final int NUM_MESSAGES = 1000;
    private static final int NUM_PARTITIONS = 10;

    @ClusterTests({
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=snappy"}),
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=gzip"}),
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=lz4"}),
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=zstd"}),
        @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT}, tags = {"compression=none"})
    })
    void testCompressedTopic(ClusterInstance cluster) throws InterruptedException {
        String compressionType = cluster.config().tags().stream()
            .filter(t -> t.startsWith("compression="))
            .map(t -> t.substring("compression=".length()))
            .findFirst().orElseThrow();

        cluster.createTopic("compression-test-" + compressionType, NUM_PARTITIONS, (short) 1);

        try (Producer<String, String> producer = cluster.producer(Map.of(
                ACKS_CONFIG, "all",
                ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType, ...))) {
            for (int i = 0; i < NUM_MESSAGES; i++)
                producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i));
            producer.flush();
        }
        // consume and assert count == NUM_MESSAGES
    }
}
```

**Comparison:**

| Aspect | Ducktape (Python) | Testcontainers (Java) |
|--------|-------------------|----------------------|
| Execution time | 1 minute 25 seconds | ~16 seconds |
| Compression handling | All 5 types in a single test run (one VerifiableProducer group with 5 producers) | One compression type per test invocation (5 separate runs) |
| Producer/Consumer | Separate JVM processes on remote nodes (`VerifiableProducer`, `ConsoleConsumer`) | In-process `KafkaProducer`/`KafkaConsumer` in test JVM |
| Cluster size | 8 nodes (1 kafka + 5 producers + 1 consumer + 1 zk/controller) | 1 container (CO_KRAFT combined) |
| ZooKeeper support | Both ZK and KRaft via `@matrix` | KRaft only |
| Test pattern | Inherits `ProduceConsumeValidateTest` base class | Self-contained, no base class needed |
| Failure diagnostics | Combined run -- harder to pinpoint which compression type failed | Isolated per type -- exact test name shows which failed |

The Ducktape version runs all 5 compression types concurrently in a single producer group, which is more of an integration/stress test. The Java version tests each compression type in isolation, which provides better failure diagnostics -- if `zstd` fails, the test name identifies it directly rather than requiring investigation of a combined run. The ~5x speedup (85 seconds vs. ~16 seconds) comes primarily from eliminating the overhead of provisioning separate JVM processes for producers and consumers, and from using Docker containers instead of VMs.

## 5. Open Items and Migration Areas

### 5.1 Versioning (KafkaVersions)

Docker images for Apache Kafka are available from version 3.7.0 onward, but only KRaft-capable versions (3.9+) are included in the framework since container tests use KRaft exclusively. Versions older than 3.9.x cannot be tested directly. ZooKeeper-based tests from Ducktape will not be migrated -- the `@ClusterTest` framework is KRaft-exclusive by design.

### 5.2 Network and Chaos Testing (Toxiproxy)

Toxiproxy replaces Trogdor for network fault injection. It supports latency injection, bandwidth throttling, connection cutting, and slow close behaviors. Testcontainers provides first-class Toxiproxy support via `ToxiproxyContainer`.

The approach is to place a Toxiproxy container between the test client and the Kafka broker containers on the same Docker network. The client connects through Toxiproxy's proxy port, and tests inject "toxics" (faults) programmatically to simulate network failures.

### 5.3 Parallel Execution

Multi-node KRaft quorum formation requires parallel container startup, which is already handled by `Startables.deepStart()`. The current Gradle configuration runs system tests sequentially (`maxParallelForks = 1`) to avoid resource contention.

Investigation is needed to determine whether system tests can run in parallel if each test uses an isolated Docker network. Each `KafkaContainerCluster` already creates its own `Network.newNetwork()`, so network-level isolation exists. The open question is whether the Docker daemon and host resources can handle multiple concurrent multi-node clusters.

### 5.4 Image Building and CI Pipeline

For CI and development branch testing, a Gradle task is needed to build an `apache/kafka:dev` image from the current source tree. The Dockerfile exists at `docker/jvm/Dockerfile`, and `docker/docker_build_test.py` can perform the build, but there is no automated Gradle task that builds and tags the image for use by `KafkaContainerCluster`.

For releases, the existing RC/release workflows (`docker_rc_release.yml`, `docker_promote.yml`) already build and push tagged images to Docker Hub.

The system property `-Dkafka.container.image=apache/kafka:dev` allows overriding the default image at runtime, so the Gradle task only needs to build the image; the test framework will pick it up via the property.

### 5.5 Log Collection

On test failure, container logs should be dumped to files organized per node. The proposed structure is:

```
build/systemtests/logs/<testName>/<timestamp>/kafka-<nodeId>.log
```

Implementation approach: add an `AfterEachCallback` to `ContainerClusterInvocationContext` that checks `context.getExecutionException()`. If the test failed, iterate over all containers and write their logs to files. This pattern is similar to what strimzi-test-container does for log preservation.

### 5.6 Public Test API for Downstream Projects

Currently, `ContainerClusterInstance` is package-private and `KafkaContainerCluster` is public but not intended for external consumption. Downstream projects such as Strimzi could benefit from depending on `org.apache.kafka:kafka-test-common-runtime` and using Kafka's container infrastructure directly, potentially replacing the standalone `strimzi-test-container` library.

**Proposed approach:**
1. First stabilize the API within Kafka's own system tests.
2. Mark the public-facing classes as `@Evolving` once the API is stable.
3. Version and document the API for downstream consumers.

**Alternative:** Keep strimzi-test-container as a thin wrapper that delegates to Kafka's test API for the underlying container management.

### 5.7 Dynamic Quorum Scaling

The current implementation uses static quorum configuration: the number of controllers and brokers is fixed at cluster creation time. KIP-853 (dynamic add/remove of controllers) will require support in the container framework for adding and removing controller containers from a running cluster.

### 5.8 Vagrant and VMs

No use cases have been identified that strictly require VMs. Docker container infrastructure is simpler than managing multiple VMs with Vagrant. This is a deliberate trade-off: the proposal accepts losing VM-level testing (kernel differences, disk I/O characteristics, true network isolation) in favor of container simplicity, portability, and speed.

### 5.9 Kafka Connect

A Connect container is needed: a worker running in its own container on the same Docker network as the Kafka brokers. Connectors can be mounted via Docker volumes or baked into a custom image. The Connect worker connects to brokers using the internal listener (`INTERNAL://kafka-<nodeId>:9094`).

### 5.10 Kafka Streams

In Ducktape, Streams applications run as separate JVM processes launched via SSH on Vagrant VMs. The container equivalent is to run the Streams application as its own container on the same Docker network.

Options:
- Build a small Docker image containing the Streams application JAR.
- Use a generic JVM container and copy the JAR into it via Testcontainers' `Transferable` API.

The container approach is arguably better than Ducktape's process model: it provides proper isolation, an independent lifecycle, and restart capability without SSH.

### 5.11 Verifiable Producer and Consumer

In Ducktape, the verifiable producer and consumer are standalone JVM tools that write structured JSON to stdout. In the container approach, these are not needed for most cases -- tests use in-process Java clients directly with full assertion capabilities.

For cases where producing or consuming from inside the Docker network is required (e.g., testing internal listener behavior), `execInContainer()` with the existing verifiable tools is available.

### 5.12 Console Consumer and Producer

Console tools are straightforward to test using `execInContainer()`:

```java
container.execInContainer(
    "/opt/kafka/bin/kafka-console-consumer.sh",
    "--bootstrap-server", "localhost:9094",
    "--topic", topicName,
    "--from-beginning",
    "--max-messages", "10"
);
```

### 5.13 Kerberos (MiniKdc)

Use a KDC Docker container (e.g., `gcavalcante8808/krb5-server`) on the same Docker network as the Kafka containers. Configure Kafka containers with `krb5.conf` and keytab files via the Testcontainers `Transferable` API. The KDC container manages the Kerberos realm and principal creation.

### 5.14 SASL/SSL

PLAIN and SCRAM-SHA-256/512 mechanisms already work via Kafka container environment variable configuration. The PoC includes SASL/PLAINTEXT and SCRAM support with automatic user creation via `kafka-configs.sh` after cluster startup.

For TLS/SSL, a decision is needed between two approaches:

| Approach | Pros | Cons |
|---|---|---|
| BouncyCastle (in-process) | Fast, no external dependency, pure Java | Additional library dependency, less production-like |
| keytool/openssl (in-container) | Proven, matches production setup, no new dependencies | Slower, requires executing commands in containers |

The recommendation is to evaluate both approaches during Phase 2 and select based on performance and reliability in CI.

### 5.15 Trogdor Fault Injection Mapping

| Trogdor Fault | Container Equivalent |
|---|---|
| `ProcessStopFaultSpec` | `stopBroker()` / `startBroker()` via Docker stop/start |
| `NetworkPartitionFaultSpec` | Toxiproxy `cut()` toxic, or `docker network disconnect` / `docker network connect` |
| `DegradedNetworkFaultSpec` | Toxiproxy `latency`, `bandwidth`, `slow_close` toxics |
| `FilesUnreadableFaultSpec` (Kibosh) | Kibosh is a FUSE filesystem, making it harder to replicate. Options: mount a volume and `chmod` to make files unreadable, or run Kibosh inside the container if FUSE is available |

### 5.16 JMX Monitoring

Expose the JMX port from Kafka containers via environment variables (`KAFKA_JMX_PORT`, `KAFKA_JMX_OPTS`) and add the JMX port to `withExposedPorts()`. From the host, connect using `JMXConnectorFactory` with the Docker-mapped port to query MBeans and verify metrics.

### 5.17 Performance Benchmarks

Two approaches are available:

- **Host-side:** Java client with timing and throughput measurement, providing precise control over measurement methodology.
- **In-container:** `execInContainer()` for `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh`, which mirrors the Ducktape approach more closely.

The in-container approach is recommended for parity with existing Ducktape benchmarks. Host-side measurement can supplement this for cases where finer-grained control is needed.

### 5.18 Test Specs (JSON Workloads)

Ducktape uses JSON spec files to define test workloads. In the container framework, `@ClusterTemplate` methods replace JSON specs entirely. Test configuration is generated programmatically in Java using `ClusterConfig.Builder`, which provides type safety, IDE support, and the ability to compute configurations dynamically.

### 5.19 Downstream Considerations

- Tests that rely on published Docker images cannot run before an RC is ready. This is solved by the local image build task proposed in Section 5.4.
- There is no ability to test upgrades from versions older than 3.9.x.
- ZooKeeper-based tests from Ducktape will not be migrated. ZooKeeper mode has been removed from Kafka, and the `@ClusterTest` framework is KRaft-exclusive.

### 5.20 Matrix Testing

The `containerImages` attribute on `@ClusterTest` accepts multiple image tags, producing a Cartesian product of test invocations. The `containerImageSource` attribute allows dynamic resolution from a static method on the test class, similar to Ducktape's `@matrix` decorator.

Combined with the `types` (KRAFT, CO_KRAFT) and `executionModes` (IN_MEMORY, CONTAINER) attributes, this produces full combinatorial test matrices. For example:

```java
@ClusterTest(
    types = {Type.KRAFT, Type.CO_KRAFT},
    containerImageSource = "crossVersionImages"
)
void testProduceConsume(ClusterInstance cluster) { ... }
```

This generates one test invocation per combination of type and container image, automatically covering all cross-version and topology permutations.

## 6. Migration Plan

### Phase 0 -- PoC (Completed)

- `KafkaContainerCluster` with parallel startup, Docker stop/start per broker, SASL support.
- `ContainerClusterInstance` implementing `ClusterInstance` for container-backed clusters.
- `ContainerClusterInvocationContext` for JUnit 5 lifecycle management.
- `ExecutionMode.CONTAINER` integrated into `ClusterTestExtensions`.
- `KafkaVersions` with centralized version management.
- `containerImages` and `containerImageSource` attributes on `@ClusterTest`.
- `ClusterConfig.Builder.copy()` for Cartesian product generation.
- Three migrated tests: `BounceST`, `CompressionST`, `CrossVersionST`.

### Phase 1 -- CI-Ready Foundation

- Log collection on test failure (`AfterEachCallback` that dumps container logs to files).
- Gradle task to build `apache/kafka:dev` from the current branch source tree.
- CI pipeline integration: run `systemtests` as a separate CI stage gated by Docker availability.
- First cross-version test using `KafkaVersions` in CI.

### Phase 2 -- Core Test Migration

- Client tests: consumer group tests, truncation tests, share group tests.
- Replication and partition reassignment tests.
- Quota and throttling tests.
- Security tests (SASL/SSL -- the framework already supports SASL; TLS needs certificate setup).
- Dynamic quorum tests (KIP-853 -- add/remove controllers).

### Phase 3 -- Fault Injection

- Toxiproxy integration as a first-class component (replaces Trogdor network faults).
- Filesystem fault injection (Kibosh equivalent or volume-level approaches).

### Phase 4 -- Ecosystem

- Kafka Connect container and Connect integration tests.
- Kafka Streams container and Streams integration tests.
- Kerberos (KDC container) for GSSAPI authentication tests.

### Phase 5 -- Performance

- JMX port exposure and metric verification tests.
- Benchmark tests using `execInContainer()` for `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh`.

### Phase 6 -- Public API

- Stabilize `KafkaContainerCluster` and related classes as a public API.
- Annotate with `@Evolving` and version for downstream consumers.
- Document the API for downstream projects such as Strimzi.

## 7. Trade-offs

| Aspect | Advantage | Limitation |
|---|---|---|
| Language | Single language (Java) for all tests | Lose Python flexibility for scripting |
| Infrastructure | Docker containers (seconds to start) | No VM-level testing (Vagrant) |
| Version coverage | 3.9.x+ via Docker images | Cannot test pre-3.9 or ZooKeeper mode |
| Client testing | In-process Java clients (fast, direct assertions) | Cannot test older client JARs without extra work |
| Framework reuse | Same `@ClusterTest` annotations for integration and system tests | Container tests are slower than in-memory |
| Fault injection | Toxiproxy (well-supported, Docker-native) | Kibosh/FUSE faults harder to replicate |
| Downstream | Potential public API for Strimzi and other projects | API stability commitment required |

## 8. Conclusion

This proposal outlines a path to migrate Kafka's system tests from Python/Ducktape to Java/Testcontainers. The completed proof-of-concept demonstrates that the approach is feasible: multi-node KRaft clusters start reliably in containers, rolling restarts work correctly, cross-version testing is straightforward, and the entire test authoring experience reuses the existing `@ClusterTest` framework that Kafka developers already know.

The migration eliminates the Python/Vagrant dependency, reduces cluster provisioning time from minutes to seconds, and consolidates the test stack into a single language. The phased plan allows incremental migration without disrupting existing tests, and the open items identified in this document provide a clear roadmap for community discussion and prioritization.

Feedback on the open items -- particularly around fault injection strategy (Section 5.2), TLS certificate generation approach (Section 5.14), and public API scope (Section 5.6) -- is welcome on the dev mailing list.