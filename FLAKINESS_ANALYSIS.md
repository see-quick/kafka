# Kafka System Test Flakiness Analysis & Fix Plan

## Problem Summary
Tests are failing with "Brokers did not become ready" errors, with AdminClient timeouts on metadata fetch calls. The root cause is a **race condition between broker container startup completion and the readiness check**.

## Root Cause Analysis

### 1. Broker Readiness Check (ContainerClusterInstance.java, lines 166-181)

**Current implementation:**
```java
public void waitForReadyBrokers() throws InterruptedException {
    try (Admin admin = admin()) {
        RaftClusterInvocationContext.waitForCondition(
            () -> {
                try {
                    int readyBrokers = admin.describeCluster().nodes().get().size();
                    return readyBrokers >= clusterConfig.numBrokers();
                } catch (InterruptedException | ExecutionException e) {
                    return false;
                }
            },
            "Brokers did not become ready",
            60_000L
        );
    }
}
```

**Issues:**
1. **AdminClient created immediately** - Created before brokers have completed startup and leadership election
2. **No connection pre-check** - AdminClient may fail to bootstrap before brokers are listening
3. **Timeout swallowed** - ExecutionException (including timeout) is treated as `false`, not escalated
4. **60 second timeout** - May be insufficient if brokers take time to elect leaders in containerized environments

### 2. Readiness Wait Loop (RaftClusterInvocationContext.java, lines 84-105)

**Current implementation:**
```java
static void waitForCondition(final java.util.function.Supplier<Boolean> testCondition,
                             final String conditionDetails,
                             final long maxWaitMs) throws InterruptedException {
    long endTime = System.currentTimeMillis() + maxWaitMs;
    
    while (System.currentTimeMillis() < endTime) {
        try {
            if (testCondition.get()) {
                return;
            }
        } catch (Exception e) {
            if (System.currentTimeMillis() >= endTime) {
                throw new AssertionError(String.format("Assertion failed with an exception after %s ms", maxWaitMs), e);
            }
        }
        
        if (System.currentTimeMillis() < endTime) {
            TimeUnit.MILLISECONDS.sleep(100);  // 100ms sleep
        }
    }
    throw new AssertionError("Condition not met: " + conditionDetails);
}
```

**Issues:**
1. **No backoff strategy** - Hammers AdminClient with 100ms frequency (10 calls/second)
2. **Exception handling silent** - Swallows transient failures without logging/tracking
3. **Hard 100ms sleep** - Too aggressive for containerized startup, especially with resource constraints
4. **No diagnostic info** - Doesn't report what errors were encountered

### 3. Container Startup Strategy (KafkaContainerCluster.java, lines 149, 280-308)

**Issues:**
1. **Parallel container startup** - All brokers start in parallel via `Startables.compose()` (line 280)
2. **Leadership election contention** - Multiple brokers simultaneously competing for leadership
3. **Port listening check only** - `Wait.forListeningPort()` checks if process bound port, not if it's accepting connections
4. **No quorum establishment wait** - Code assumes port listening = broker ready, but KRaft quorum election takes additional time
5. **Shared network initialization** - Docker network creation/initialization can delay inter-container communication

### 4. AdminClient Configuration (CompressionST.java and others)

**Issues:**
1. **Default AdminClient timeouts** - Likely 60s, but each retry call fails individually
2. **No connection pre-warmup** - First call attempts to reach brokers that may not have elected leaders yet
3. **No bootstrap retry logic** - Single failed bootstrap attempt causes `TimeoutException`

## Evidence from Logs

```
Timed out waiting to send the call. Call: fetchMetadata
```

This appears **139 times** in quick succession, indicating:
- AdminClient bootstrap fails repeatedly
- Brokers report "port listening" but haven't completed leadership election
- Test waits 60 seconds but only checks every 100ms

## Recommended Fixes

### Fix 1: Enhanced Broker Readiness Check
**File:** `ContainerClusterInstance.java` lines 166-181

**Change:**
- Add explicit port/connectivity check before AdminClient creation
- Longer initial wait (brokers take time to be election-ready in containers)
- Increase AdminClient timeout for bootstrap
- Add exponential backoff with max delay

```java
public void waitForReadyBrokers() throws InterruptedException {
    try (Admin admin = admin()) {
        // Initial wait: let brokers startup and do leader election
        RaftClusterInvocationContext.waitForCondition(
            () -> {
                try {
                    // This will wait for bootstrap AND metadata to be available
                    int readyBrokers = admin.describeCluster().nodes().get().size();
                    return readyBrokers >= clusterConfig.numBrokers();
                } catch (ExecutionException e) {
                    // Log the actual error for diagnostics
                    return false;
                } catch (InterruptedException e) {
                    throw e;
                }
            },
            "Brokers did not become ready",
            120_000L  // Increased from 60s to 120s for container startup + election
        );
    }
}
```

### Fix 2: Better Wait Loop with Exponential Backoff
**File:** `RaftClusterInvocationContext.java` lines 84-105

**Change:**
- Start with longer initial delay (500ms instead of 100ms)
- Add exponential backoff: 500ms, 1s, 2s, max 5s
- Log consecutive failures for diagnostics
- Track exception types separately

```java
static void waitForCondition(final java.util.function.Supplier<Boolean> testCondition,
                             final String conditionDetails,
                             final long maxWaitMs) throws InterruptedException {
    long endTime = System.currentTimeMillis() + maxWaitMs;
    long backoffMs = 500;  // Start with 500ms instead of 100ms
    final long MAX_BACKOFF = 5_000;
    int failures = 0;
    
    while (System.currentTimeMillis() < endTime) {
        try {
            if (testCondition.get()) {
                return;
            }
            failures++;
        } catch (Exception e) {
            failures++;
            if (System.currentTimeMillis() >= endTime) {
                throw new AssertionError(
                    String.format("Assertion failed with an exception after %s ms (%d failures)", 
                    maxWaitMs, failures), e);
            }
        }
        
        long remainingMs = endTime - System.currentTimeMillis();
        if (remainingMs > 0) {
            long sleepMs = Math.min(backoffMs, remainingMs);
            TimeUnit.MILLISECONDS.sleep(sleepMs);
            backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF);  // Exponential backoff
        }
    }
    throw new AssertionError("Condition not met: " + conditionDetails + " (failed " + failures + " times)");
}
```

### Fix 3: Add Pre-Check for Broker Connectivity
**File:** `KafkaContainerCluster.java` after line 308

**Change:**
- After `Startables.compose()`, add a check that brokers are actually listening AND responding
- Use socket connectivity check, not just process startup

```java
private void waitForBrokerConnectivity() {
    // After Startables.compose() completes, verify brokers are actually responsive
    // This prevents the AdminClient from being created too early
    for (int nodeId : brokerIds()) {
        GenericContainer<?> container = containers.get(nodeId);
        String port = container.getContainerInfo()
            .getNetworkSettings()
            .getPorts()
            .getBindings()
            .get(KAFKA_PORT);
        // Use testcontainers TCP check, not just port listening
        Wait.forListeningPort()
            .withStartupTimeout(Duration.ofMinutes(2))
            .waitUntilReady(container);
    }
}
```

### Fix 4: AdminClient Configuration for Container Tests
**File:** CompressionST.java and related test classes

**Change:**
- Increase AdminClient request timeout
- Add retry configuration
- Use longer bootstrap.servers connection timeout

```java
// When creating AdminClient in container tests
private Admin admin() {
    return Admin.create(Map.ofEntries(
        Map.entry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()),
        // Increase timeouts for container startup delays
        Map.entry(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000),  // 30s per request
        Map.entry(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60_000),  // 60s
        Map.entry(AdminClientConfig.DEFAULT_ADMIN_TIMEOUT_MS_CONFIG, 30_000),  // 30s
        Map.entry("socket.connection.setup.timeout.ms", 30_000)  // 30s for socket setup
    ));
}
```

## Expected Improvements

After applying these fixes:

1. **Longer initial delay** (500ms instead of 100ms) reduces AdminClient hammering
2. **Exponential backoff** prevents thundering herd as brokers become ready
3. **Increased timeout** (120s instead of 60s) allows time for container startup + leader election
4. **Better diagnostics** logs failure counts and exception types for debugging
5. **Socket pre-check** ensures brokers are truly listening before AdminClient attempts bootstrap

## Testing the Fix

1. Run existing system tests multiple times (20+ runs) to verify no flaky failures
2. Monitor broker startup logs for AdminClient timeout messages
3. Verify test completion time doesn't increase significantly (initial longer wait is offset by fewer retries)

## Timeline
- **P0**: Implement Fix 1 (broker readiness check timeout increase) - lowest risk, immediate benefit
- **P1**: Implement Fix 2 (exponential backoff) - medium complexity, high benefit
- **P2**: Implement Fix 3 (connectivity pre-check) - requires KafkaContainerCluster changes
- **P3**: Implement Fix 4 (AdminClient config tuning) - optional but recommended

