Hello Kafka devs,

Before I dive into proposal, I would like to start that main motivation comes from using Kafka STs over multiple years with aim to make them more effective and user-friendly for all Kafka folks out there.

---

WHAT

I would like to propose to migrate Apache Kafka system tests from the current Python/Ducktape framework to Java/Testcontainers-based approach.

The goal is to unify the test stack around Java (i.e., primary language around Kafka), reduce overall infra complexity, improve execution speed, and integrate system tests into existing @ClusterTest framework.

WHY / WHY NOT

The main motivation is that the Kafka community is mainly Java-centric, and having system tests written in Python using the Ducktape framework might create a barrier for contributors who are not familiar with Python, Ducktape's abstraction model, or its test runner conventions. This basically requires contributors to learn a second language and a niche testing framework to write or modify STs, which slows overall development.

Another important point is framework unification. By implementing container-based execution as an adapter within the existing @ClusterTest integration framework, contributors can write system tests using the same familiar annotations and patterns they already use for integration tests. The only difference is the execution mode (i.e., ExecutionMode.CONTAINER instead of ExecutionMode.IN_MEMORY), which completely eliminates the need to learn a separate framework.

In terms of maintenance, consolidating on a single language and test framework reduces the overall maintenance burden. Bug fixes, framework improvements, and tooling enhancements benefit both integration and system tests simultaneously, and there is no need to maintain both Python and Java test infrastructure. However, this also means that changes to the shared test framework (e.g., ClusterTestExtensions, ClusterConfig, ClusterInstance) could affect both integration and system tests, so contributors modifying the framework would need to verify that system tests are not broken, which may require running them as part of the validation process.

While I can also see another benefit in test execution speed, it may vary depending on the test scenario (as Ducktape can also run within containers, not just provisioning VMs). As a concrete example, the compression test -- which produces and consumes 1,000 messages per compression type (snappy, gzip, lz4, zstd, none) -- takes 1 minute and 25 seconds in Ducktape versus ~12 seconds with Testcontainers. That is roughly a ~7x speedup. Note that the comparison is not strictly apples-to-apples: Ducktape launches VerifiableProducer/ConsoleConsumer as separate JVM processes (with SSH and process startup overhead), while the Testcontainers version uses in-process Java clients with no process spawn cost. The test logic is comparable (same message count, same compression types), but the execution model is different, which contributes to the speedup. That said, this speedup is not universal -- tests dominated by actual Kafka processing time (e.g., large-scale replication, long-running stress tests) will see a smaller relative improvement, since the bottleneck is the workload itself rather than the infrastructure overhead.

This proposal is not without trade-offs. Ducktape can target both VMs and containers, which gives it the ability to test scenarios that require full OS-level isolation (e.g., kernel networking, disk I/O behavior under specific filesystems, or multi-host deployments across separate machines). The Testcontainers approach cannot replace these scenarios. Moreover, containers share a host kernel, so certain classes of faults (e.g., FUSE-based filesystem injection via Kibosh) are harder to simulate.

Another disadvantage is that version coverage is narrower, as Apache Kafka Docker images are only available from 3.7.x onward.

The KRaft-exclusive framework (i.e., Kafka's integration test framework) limits testing to versions 3.9 and above, which means ZooKeeper-based versions and anything prior to 3.9 cannot be tested. This is a deliberate trade-off: KRaft was marked production-ready in Kafka 3.6.0, Docker images are available from 3.7.0, and ZooKeeper mode was removed entirely in Kafka 4.0 -- so investing in ZK-based test infrastructure for a forward-looking framework is not justified.

It is also worth noting that the goal is not to migrate every single Ducktape test. Some tests that rely on pre-3.9 version coverage or OS-level isolation may remain in Ducktape, and we can possibly mirror them in Testcontainers where feasible. The objective is to migrate as many tests as possible to the new framework and to use it as the default for any new system tests going forward, unless the test scenario requires capabilities that only Ducktape can provide.

HOW

If we agree that this direction makes sense, the implementation plan would cover three areas:

(i.) Tests that can be rewritten now - a catalog of existing Ducktape tests that can be migrated to the @ClusterTest/Testcontainers framework with current capabilities (e.g., compression tests, basic produce/consume, bounce tests, cross-version sanity).

(ii.) Features that need to be added to @ClusterTest - a list of framework enhancements required to support more advanced test scenarios (e.g., TLS/SASL support, network fault injection as a Trogdor replacement, Kafka Connect and Kafka Streams abstractions, JMX monitoring, dynamic quorum scaling, MiniKdc integration, log collection, console producer/consumer wrappers).

(iii.) A mechanism to run everything easily - a unified way to execute both the remaining Python/Ducktape tests and the new Java/Testcontainers tests, so that CI pipelines and developers have a single-entry point regardless of which framework a given test uses.

I would like to know if the motivation outlined above is fair enough to consider moving forward with such a change. If we agree, I can follow up with the detailed breakdown for each of these three areas.

Thank you in advance for your replies.

Cheers,

Maros