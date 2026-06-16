# Proposal: Migrating Kafka System Tests from Ducktape to Testcontainers

## 1. Summary

This document proposes migrating Apache Kafka's system tests from the current Python/Ducktape framework to a Java/Testcontainers-based approach.
The goal is to unify the test stack around Java, reduce infrastructure complexity, improve execution speed, and integrate system tests into the existing `@ClusterTest` framework.
A proof-of-concept implementation has been completed, demonstrating that multi-node container-based Kafka clusters can be managed and tested using the same annotation-driven patterns already familiar to Kafka developers from integration tests.

## 2. Motivation

### 2.1 Language Barrier

The Kafka community is overwhelmingly Java-centric.
The existing system tests are written in Python using the Ducktape framework, which creates a barrier for contributors who are not familiar with Python, Ducktape's service abstraction model, or its test runner conventions.
Requiring contributors to learn a second language and a niche testing framework discourages participation and slows development.

### 2.2 Execution Speed

Docker containers start in seconds, compared to minutes for full VMs.
A three-node Kafka cluster can be provisioned and running in under 30 seconds using Testcontainers, versus several minutes with Vagrant.
This difference compounds across the full test suite and significantly affects the developer feedback loop.

### 2.3 Framework Unification

By implementing container-based execution as an adapter within the existing `@ClusterTest` integration test framework, newcomers can write system tests using the same familiar annotations and patterns they already use for integration tests.
The only difference is the execution mode: `CONTAINER` instead of `IN_MEMORY`.
This eliminates the need to learn a separate framework.

### 2.4 Maintenance Consolidation

Consolidating on a single language and test framework reduces the overall maintenance burden.
Bug fixes, framework improvements, and tooling enhancements (IDE support, debugging, profiling) benefit both integration and system tests simultaneously.
There is no longer a need to maintain parallel Python and Java test infrastructure.
However, this also means that changes to the shared test framework could affect both integration and system tests.
Contributors modifying the framework would need to verify that system tests are not broken, which may require running them as part of the validation process.

### 2.5 Limitations of Containers vs. VMs

This migration is not without trade-offs.
Ducktape can target both VMs and containers, which gives it the ability to test scenarios that require full OS-level isolation, such as kernel-level networking, disk I/O behavior under specific filesystems, or multi-host deployments across separate machines.
A container-only approach cannot replicate these scenarios.
Additionally, containers share the host kernel, so certain classes of faults (e.g., FUSE-based filesystem injection via Kibosh) are harder to simulate.
The version coverage is also narrower: Apache Kafka Docker images are available from 3.7.0 onward, and the KRaft-exclusive framework limits testing to 3.9+, meaning ZooKeeper-based versions and versions prior to 3.9 cannot be tested.

That said, the benefits outlined above outweigh these limitations for the majority of system test scenarios.
The goal is not necessarily to migrate every single Ducktape test.
Some tests that rely on VM-level capabilities or pre-3.9 version coverage may remain in Ducktape.
The objective is to migrate as many tests as possible to the new framework, covering the vast majority of the test surface, while keeping Ducktape as a fallback for the small number of scenarios that genuinely require it.

## 3. Architecture

### 3.1 Integration with the @ClusterTest Framework

The container execution mode is implemented as a new variant within the existing `ClusterTestExtensions` JUnit 5 extension.
A new `ExecutionMode.CONTAINER` has been added alongside `ExecutionMode.IN_MEMORY`.
Both modes support KRaft with isolated controller and broker roles as well as combined mode.
When the framework encounters a container execution mode, it delegates to a `ContainerClusterInstance` that manages Docker containers via Testcontainers, rather than starting in-process brokers.

From the test author's perspective, writing a system test looks identical to writing an integration test.
The same `@ClusterTest`, `@ClusterTests`, and `@ClusterTestDefaults` annotations are used.
The only visible difference is specifying `ExecutionMode.CONTAINER` and, optionally, Docker image tags for cross-version testing.

### 3.2 Cluster Management

`KafkaContainerCluster` is the core component responsible for managing multi-node Kafka clusters as Docker containers.
It handles quorum configuration, listener setup, container lifecycle, and provides operations like starting and stopping individual brokers.
Containers run on a shared Docker network and are started in parallel for fast provisioning.

### 3.3 Producers and Consumers

Tests use in-process Java clients running in the test JVM.
The test creates standard `KafkaProducer` and `KafkaConsumer` instances that connect to the containerized brokers via Docker-mapped external ports.
This is the same approach Ducktape uses with in-process Python clients.
For testing console tools (e.g., `kafka-console-consumer.sh`), the `execInContainer()` API can run scripts inside the broker containers.

### 3.4 Cross-Version Testing

A centralized `KafkaVersions` class defines available Docker image versions.
Tests can reference this via annotation attributes that dynamically resolve the version list at runtime.
This makes it straightforward to test the current development branch client JARs against older broker versions.
Testing older client JARs against newer brokers is not straightforward without separate classloaders or containerized client applications, which is an inherent limitation of the in-process client model.

### 3.5 Module Structure

The implementation is organized across three modules.
The `systemtests` module contains all migrated system tests.
Container framework classes live in `test-common/test-common-runtime`.
Annotations and configuration classes are in `test-common/test-common-internal-api`.
System tests are gated behind the `containerTests` Gradle property and run sequentially by default.

## 4. PoC Status (Phase 0)

The proof-of-concept is complete and functional.

Multi-node containerized Kafka cluster management with parallel startup has been implemented, supporting KRaft mode with configurable broker and controller counts, combined and isolated roles, SASL/PLAINTEXT and SASL/SCRAM security protocols, per-node process role assignment, and Docker stop/start for individual brokers that preserves container filesystem across restarts.

A full `ClusterInstance` implementation backed by Docker containers provides bootstrap servers, broker and controller IDs, shutdown and start operations for individual brokers, ready-state waiting, and all client factory methods.
Methods requiring in-process JVM access to server internals throw `UnsupportedOperationException` by design, since those internals are not accessible from outside the container.

Three tests have been migrated as proof-of-concept:

- **BounceST** validates rolling restarts: produce messages, perform a rolling restart of all brokers, produce more messages, then consume and verify all messages are present.
- **CompressionST** produces and consumes messages using all compression types (snappy, gzip, lz4, zstd, none) across both KRaft and combined-mode topologies.
- **CrossVersionST** produces and consumes messages using the current client against broker containers running each released Kafka version from 3.9 through 4.2.

As a concrete comparison, the Ducktape `CompressionTest` takes about 1 minute 25 seconds and requires 8 nodes.
The Testcontainers `CompressionST` runs in roughly 16 seconds with a single container.
The speedup comes primarily from eliminating separate JVM processes for producers and consumers and from using Docker containers instead of VMs.

## 5. Open Items

### 5.1 Network and Chaos Testing

Toxiproxy is proposed as the replacement for Trogdor for network fault injection.
It supports latency injection, bandwidth throttling, connection cutting, and slow close behaviors.
Testcontainers provides first-class Toxiproxy support.
The approach is to place a Toxiproxy container between the test client and the Kafka broker containers on the same Docker network, allowing tests to inject faults programmatically.

### 5.2 Parallel Execution

Each `KafkaContainerCluster` already creates its own isolated Docker network.
The open question is whether the Docker daemon and host resources can handle multiple concurrent multi-node clusters, which would allow system tests to run in parallel.

### 5.3 Image Building and CI Pipeline

For CI and development branch testing, a Gradle task is needed to build a development image from the current source tree.
The Dockerfile already exists, but there is no automated Gradle task that builds and tags the image.
A system property allows overriding the default image at runtime, so the Gradle task only needs to build the image and the test framework will pick it up.

### 5.4 Log Collection

On test failure, container logs should be dumped to files organized per node.
The implementation would check whether the test failed and, if so, iterate over all containers and write their logs to files for post-mortem analysis.

### 5.5 Kafka Connect

A Connect container is needed: a worker running in its own container on the same Docker network as the Kafka brokers.
Connectors can be mounted via Docker volumes or baked into a custom image.

### 5.6 Kafka Streams

In Ducktape, Streams applications run as separate JVM processes launched via SSH.
The container equivalent is to run the Streams application as its own container on the same Docker network.
This approach provides better isolation, an independent lifecycle, and restart capability without SSH.

### 5.7 Security Testing

PLAIN and SCRAM mechanisms already work via Kafka container environment variable configuration.
For TLS/SSL, a decision is needed between in-process certificate generation (e.g., BouncyCastle) and in-container generation via keytool/openssl.
Kerberos testing would use a KDC Docker container on the same network as the Kafka containers.

### 5.8 Fault Injection Mapping

Docker stop/start replaces Trogdor's process stop faults.
Toxiproxy replaces network partition and degraded network faults.
Kibosh (FUSE-based filesystem faults) is harder to replicate in containers and may require alternative approaches such as volume-level permissions changes.

### 5.9 JMX and Performance

JMX ports can be exposed from Kafka containers for metric verification.
Performance benchmarks can run either host-side with Java clients or in-container using the existing perf test scripts.

### 5.10 Dynamic Quorum Scaling

The current implementation uses static quorum configuration.
KIP-853 (dynamic add/remove of controllers) will require support for adding and removing controller containers from a running cluster.

### 5.11 Public API for Downstream Projects

Downstream projects such as Strimzi could benefit from using Kafka's container infrastructure directly.
The proposed approach is to first stabilize the API within Kafka's own system tests, then mark public-facing classes as evolving, and finally version and document the API for downstream consumers.

## 6. Migration Plan

### Phase 1: CI-Ready Foundation

Log collection on test failure, a Gradle task to build development images, CI pipeline integration, and the first cross-version test running in CI.

### Phase 2: Core Test Migration

Client tests (consumer groups, truncation, share groups), replication and partition reassignment tests, quota and throttling tests, security tests, and dynamic quorum tests.

### Phase 3: Fault Injection

Toxiproxy integration as a first-class component and filesystem fault injection approaches.

### Phase 4: Ecosystem

Kafka Connect container and integration tests, Kafka Streams container and integration tests, Kerberos authentication tests.

### Phase 5: Performance

JMX metric verification tests and benchmark tests using the existing perf test scripts.

### Phase 6: Public API

Stabilize and version the container infrastructure API for downstream projects.

## 7. Conclusion

This proposal outlines a path to migrate Kafka's system tests from Python/Ducktape to Java/Testcontainers.
The completed proof-of-concept demonstrates that the approach is feasible: multi-node KRaft clusters start reliably in containers, rolling restarts work correctly, cross-version testing is straightforward, and the entire test authoring experience reuses the existing `@ClusterTest` framework that Kafka developers already know.

The migration eliminates the Python/Vagrant dependency, reduces cluster provisioning time from minutes to seconds, and consolidates the test stack into a single language.
The phased plan allows incremental migration without disrupting existing tests, and the open items identified in this document provide a clear roadmap for community discussion and prioritization.

Feedback on the open items, particularly around fault injection strategy, TLS certificate generation approach, and public API scope, is welcome on the dev mailing list.