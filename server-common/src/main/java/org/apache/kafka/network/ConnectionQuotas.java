/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.network;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.network.ListenerReconfigurable;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.server.KafkaConfig;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.quota.QuotaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toMap;

/**
 * Java port of ConnectionQuotas with Optional-based updateIpConnectionRateQuota.
 */
public class ConnectionQuotas implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionQuotas.class);
    private static final String MetricsGroup = "kafka.network";
    private static final String ListenerMetricTag = "listener";

    volatile private int defaultMaxConnectionsPerIp;
    volatile private Map<InetAddress, Integer> maxConnectionsPerIpOverrides;
    volatile private int brokerMaxConnections;

    private final ListenerName interBrokerListenerName;
    private final Object countsLock = new Object();

    // guarded by countsLock
    private final java.util.HashMap<InetAddress, Integer> counts = new java.util.HashMap<>();
    private final java.util.HashMap<ListenerName, Integer> listenerCounts = new java.util.HashMap<>();
    // guarded by countsLock
    final java.util.HashMap<ListenerName, ListenerConnectionQuota> maxConnectionsPerListener = new java.util.HashMap<>();

   volatile private int totalCount = 0;

    // updates to defaultConnectionRatePerIp or connectionRatePerIp must be synchronized on countsLock
   volatile private int defaultConnectionRatePerIp = QuotaConfig.IP_CONNECTION_RATE_DEFAULT;
    private final ConcurrentHashMap<InetAddress, Integer> connectionRatePerIp = new ConcurrentHashMap<>();

    private final Metrics metrics;
    private final Time time;
    private final KafkaConfig config;

    // broker-wide connection creation rate sensor
    private final Sensor brokerConnectionRateSensor;
    private final long maxThrottleTimeMs;

    public ConnectionQuotas(KafkaConfig config, Time time, Metrics metrics) {
        this.config = Objects.requireNonNull(config, "config");
        this.time = Objects.requireNonNull(time, "time");
        this.metrics = Objects.requireNonNull(metrics, "metrics");

        this.defaultMaxConnectionsPerIp = config.maxConnectionsPerIp();
        this.maxConnectionsPerIpOverrides = resolveOverrides(config.maxConnectionsPerIpOverrides());
        this.brokerMaxConnections = config.maxConnections();
        this.interBrokerListenerName = config.interBrokerListenerName();

        this.brokerConnectionRateSensor = getOrCreateConnectionRateQuotaSensor(
            config.maxConnectionCreationRate(),
            ConnectionQuotaEntity.brokerQuotaEntity()
        );
        this.maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(config.quotaConfig().quotaWindowSizeSeconds());
    }

    private static Map<InetAddress, Integer> resolveOverrides(Map<String, Integer> raw) {
        return raw.entrySet().stream().collect(toMap(
            e -> {
                try {
                    return InetAddress.getByName(e.getKey());
                } catch (UnknownHostException ex) {
                    throw new IllegalArgumentException("Unable to resolve " + e.getKey(), ex);
                }
            },
            Map.Entry::getValue
        ));
    }

    public void inc(ListenerName listenerName, InetAddress address, com.yammer.metrics.core.Meter acceptorBlockedPercentMeter) {
        synchronized (countsLock) {
            waitForConnectionSlot(listenerName, acceptorBlockedPercentMeter);

            recordIpConnectionMaybeThrottle(listenerName, address);
            int count = counts.getOrDefault(address, 0);
            counts.put(address, count + 1);
            totalCount += 1;

            if (listenerCounts.containsKey(listenerName)) {
                listenerCounts.put(listenerName, listenerCounts.get(listenerName) + 1);
            }

            int max = maxConnectionsPerIpOverrides.getOrDefault(address, defaultMaxConnectionsPerIp);
            if (count >= max) {
                throw new TooManyConnectionsException(address, max);
            }
        }
    }

    void updateMaxConnectionsPerIp(int maxConnectionsPerIp) {
        defaultMaxConnectionsPerIp = maxConnectionsPerIp;
    }

    void updateMaxConnectionsPerIpOverride(Map<String, Integer> overrideQuotas) {
        maxConnectionsPerIpOverrides = resolveOverrides(overrideQuotas);
    }

    void updateBrokerMaxConnections(int maxConnections) {
        synchronized (countsLock) {
            brokerMaxConnections = maxConnections;
            countsLock.notifyAll();
        }
    }

    void updateBrokerMaxConnectionRate(int maxConnectionRate) {
        // If a connection is already waiting due to throttling, let it finish.
        updateConnectionRateQuota(maxConnectionRate, ConnectionQuotaEntity.brokerQuotaEntity());
    }

    /**
     * Update the connection rate quota for a given IP and updates quota configs for updated IPs.
     * If an IP is given, metric config will be updated only for the given IP, otherwise
     * all metric configs will be checked and updated if required.
     *
     * @param ip                IP to update or default if empty
     * @param maxConnectionRate new connection rate, or resets entity to default if empty
     */
    public synchronized void updateIpConnectionRateQuota(Optional<InetAddress> ip, Optional<Integer> maxConnectionRate) {
        java.util.function.Predicate<MetricName> isIpConnectionRateMetric = metricName ->
            metricName.name().equals(ConnectionQuotaEntity.CONNECTION_RATE_METRIC_NAME) &&
                metricName.group().equals(MetricsGroup) &&
                metricName.tags().containsKey(ConnectionQuotaEntity.IP_METRIC_TAG);

        java.util.function.BiPredicate<KafkaMetric, Integer> shouldUpdateQuota =
            (metric, quotaLimit) -> quotaLimit != metric.config().quota().bound();

        if (ip.isPresent()) {
            InetAddress address = ip.get();
            // synchronize on counts to ensure reading quota + creating metric config is atomic
            synchronized (countsLock) {
                if (maxConnectionRate.isPresent()) {
                    LOGGER.info("Updating max connection rate override for {} to {}", address, maxConnectionRate.get());
                    connectionRatePerIp.put(address, maxConnectionRate.get());
                } else {
                    LOGGER.info("Removing max connection rate override for {}", address);
                    connectionRatePerIp.remove(address);
                }
            }
            updateConnectionRateQuota(connectionRateForIp(address), ConnectionQuotaEntity.ipQuotaEntity(address));
        } else {
            synchronized (countsLock) {
                defaultConnectionRatePerIp = maxConnectionRate.orElse(QuotaConfig.IP_CONNECTION_RATE_DEFAULT);
            }
            LOGGER.info("Updated default max IP connection rate to {}", defaultConnectionRatePerIp);

            for (Map.Entry<MetricName, KafkaMetric> e : metrics.metrics().entrySet()) {
                MetricName metricName = e.getKey();
                KafkaMetric metric = e.getValue();
                if (isIpConnectionRateMetric.test(metricName)) {
                    try {
                        InetAddress addr = InetAddress.getByName(metricName.tags().get(ConnectionQuotaEntity.IP_METRIC_TAG));
                        int quota = connectionRateForIp(addr);
                        if (shouldUpdateQuota.test(metric, quota)) {
                            LOGGER.debug("Updating existing connection rate quota config for {} to {}", metricName.tags(), quota);
                            metric.config(rateQuotaMetricConfig(quota));
                        }
                     } catch (UnknownHostException ignore) {
                        // don't fail a refresh sweep if a tag can't resolve
                    }
                }
            }
        }
    }

    /** Visible for testing */
    int connectionRateForIp(InetAddress ip) {
        return connectionRatePerIp.getOrDefault(ip, defaultConnectionRatePerIp);
    }

    void addListener(KafkaConfig config, ListenerName listenerName) {
        synchronized (countsLock) {
            if (!maxConnectionsPerListener.containsKey(listenerName)) {
                ListenerConnectionQuota newListenerQuota = new ListenerConnectionQuota(countsLock, listenerName);
                maxConnectionsPerListener.put(listenerName, newListenerQuota);
                listenerCounts.put(listenerName, 0);
                config.addReconfigurable(newListenerQuota);
                newListenerQuota.configure(config.valuesWithPrefixOverride(listenerName.configPrefix()));
            }
            countsLock.notifyAll();
        }
    }

    void removeListener(KafkaConfig config, ListenerName listenerName) {
        synchronized (countsLock) {
            ListenerConnectionQuota listenerQuota = maxConnectionsPerListener.remove(listenerName);
            if (listenerQuota != null) {
                listenerCounts.remove(listenerName);
                // once removed, no metrics will be recorded into listener's sensor → safe to remove
                listenerQuota.close();
                countsLock.notifyAll(); // wake any waiting acceptors to close cleanly
                config.removeReconfigurable(listenerQuota);
            }
        }
    }

    public void dec(ListenerName listenerName, InetAddress address) {
        synchronized (countsLock) {
            Integer count = counts.get(address);
            if (count == null) {
                throw new IllegalArgumentException("Attempted to decrease connection count for address with no connections, address: " + address);
            }
            if (count == 1) counts.remove(address);
            else counts.put(address, count - 1);

            if (totalCount <= 0) LOGGER.error("Attempted to decrease total connection count for broker with no connections");
            totalCount -= 1;

            if (maxConnectionsPerListener.containsKey(listenerName)) {
                int lc = listenerCounts.get(listenerName);
                if (lc == 0) LOGGER.error("Attempted to decrease connection count for listener {} with no connections", listenerName);
                else listenerCounts.put(listenerName, lc - 1);
            }
            countsLock.notifyAll(); // wake acceptors waiting due to listener connection limit
        }
    }

    public int get(InetAddress address) {
        synchronized (countsLock) {
            return counts.getOrDefault(address, 0);
        }
    }

    private void waitForConnectionSlot(ListenerName listenerName,
                                       com.yammer.metrics.core.Meter acceptorBlockedPercentMeter) {
        synchronized (countsLock) {
            long startThrottleTimeMs = time.milliseconds();
            long throttleTimeMs = Math.max(recordConnectionAndGetThrottleTimeMs(listenerName, startThrottleTimeMs), 0);

            if (throttleTimeMs > 0 || !connectionSlotAvailable(listenerName)) {
                long startNs = time.nanoseconds();
                long endThrottleTimeMs = startThrottleTimeMs + throttleTimeMs;
                long remainingThrottleTimeMs = throttleTimeMs;
                do {
                    try {
                        countsLock.wait(remainingThrottleTimeMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    remainingThrottleTimeMs = Math.max(endThrottleTimeMs - time.milliseconds(), 0);
                } while (remainingThrottleTimeMs > 0 || !connectionSlotAvailable(listenerName));
                acceptorBlockedPercentMeter.mark(time.nanoseconds() - startNs);
            }
        }
    }

    /** Invoked in every poll; close one LRU connection in an iteration if necessary */
    public boolean maxConnectionsExceeded(ListenerName listenerName) {
        return totalCount > brokerMaxConnections && !protectedListener(listenerName);
    }

    private boolean connectionSlotAvailable(ListenerName listenerName) {
        Integer lcount = listenerCounts.get(listenerName);
        if (lcount != null && lcount >= maxListenerConnections(listenerName)) return false;
        if (protectedListener(listenerName)) return true;
        return totalCount < brokerMaxConnections;
    }

    private boolean protectedListener(ListenerName listenerName) {
        return interBrokerListenerName.equals(listenerName) && listenerCounts.size() > 1;
    }

    private int maxListenerConnections(ListenerName listenerName) {
        ListenerConnectionQuota q = maxConnectionsPerListener.get(listenerName);
        return (q != null) ? q.maxConnections() : Integer.MAX_VALUE;
    }

    /**
     * Calculates the delay needed to bring the observed connection creation rate to listener-level limit
     * or to broker-wide limit, whichever is longer. Capped by quota window size.
     */
    private long recordConnectionAndGetThrottleTimeMs(ListenerName listenerName, long timeMs) {
        java.util.function.IntUnaryOperator recordAndGetListenerThrottleTime = (minThrottleTimeMs) -> {
            ListenerConnectionQuota listenerQuota = maxConnectionsPerListener.get(listenerName);
            if (listenerQuota == null) return 0;
            int listenerThrottleTimeMs = recordAndGetThrottleTimeMs(listenerQuota.connectionRateSensor, timeMs);
            int throttleTimeMs = Math.max(minThrottleTimeMs, listenerThrottleTimeMs);
            if (throttleTimeMs > 0) {
                listenerQuota.listenerConnectionRateThrottleSensor.record((double) throttleTimeMs, timeMs);
            }
            return throttleTimeMs;
        };

        if (protectedListener(listenerName)) {
            return recordAndGetListenerThrottleTime.applyAsInt(0);
        } else {
            int brokerThrottleTimeMs = recordAndGetThrottleTimeMs(brokerConnectionRateSensor, timeMs);
            return recordAndGetListenerThrottleTime.applyAsInt(brokerThrottleTimeMs);
        }
    }

    /**
     * Record IP throttle time on the corresponding listener. To avoid over-recording listener/broker connection rate,
     * we also un-record the listener and broker connection if the IP gets throttled.
     */
    private void updateListenerMetrics(ListenerName listenerName, long throttleMs, long timeMs) {
        if (!protectedListener(listenerName)) {
            brokerConnectionRateSensor.record(-1.0, timeMs, false);
        }
        ListenerConnectionQuota listenerQuota = maxConnectionsPerListener.get(listenerName);
        if (listenerQuota != null) {
            listenerQuota.ipConnectionRateThrottleSensor.record((double) throttleMs, timeMs);
            listenerQuota.connectionRateSensor.record(-1.0, timeMs, false);
        }
    }

    /**
     * Calculates the delay needed to bring the observed connection creation rate to the IP limit.
     * If the connection would violate the quota, un-record the connection and throw.
     * Must be called with countsLock held.
     */
    private void recordIpConnectionMaybeThrottle(ListenerName listenerName, InetAddress address) {
        int connectionRateQuota = connectionRateForIp(address);
        boolean quotaEnabled = connectionRateQuota != QuotaConfig.IP_CONNECTION_RATE_DEFAULT;
        if (quotaEnabled) {
            Sensor sensor = getOrCreateConnectionRateQuotaSensor(connectionRateQuota, ConnectionQuotaEntity.ipQuotaEntity(address));
            long timeMs = time.milliseconds();
            int throttleMs = recordAndGetThrottleTimeMs(sensor, timeMs);
            if (throttleMs > 0) {
                LOGGER.trace("Throttling $address for {} ms", throttleMs);
                // unrecord the connection since we won't accept it
                sensor.record(-1.0, timeMs, false);
                updateListenerMetrics(listenerName, throttleMs, timeMs);
                throw new ConnectionThrottledException(address, timeMs, throttleMs);
            }
        }
    }

    /**
     * Records a new connection into 'sensor' and returns throttle time in ms if quota is violated.
     */
    private int recordAndGetThrottleTimeMs(Sensor sensor, long timeMs) {
        try {
            sensor.record(1.0, timeMs);
            return 0;
        } catch (QuotaViolationException e) {
            int throttleTimeMs = (int) QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs);
            LOGGER.debug("Quota violated for sensor ({}). Delay time: {} ms", sensor.name(), throttleTimeMs);
            return throttleTimeMs;
        }
    }

    /**
     * Creates sensor for tracking the connection creation rate + quota for a given entity.
     */
    private Sensor getOrCreateConnectionRateQuotaSensor(int quotaLimit, ConnectionQuotaEntity connectionQuotaEntity) {
        Sensor existing = metrics.getSensor(connectionQuotaEntity.sensorName());
        if (existing != null) return existing;

        Sensor sensor = metrics.sensor(
            connectionQuotaEntity.sensorName(),
            rateQuotaMetricConfig(quotaLimit),
            connectionQuotaEntity.sensorExpiration()
        );
        sensor.add(connectionRateMetricName(connectionQuotaEntity), new Rate(), null);
        return sensor;
    }

    /**
     * Updates quota configuration for a given entity.
     */
    private void updateConnectionRateQuota(int quotaLimit, ConnectionQuotaEntity connectionQuotaEntity) {
        KafkaMetric metric = (KafkaMetric) metrics.metric(connectionRateMetricName(connectionQuotaEntity));
        if (metric != null) {
            metric.config(rateQuotaMetricConfig(quotaLimit));
            LOGGER.info("Updated ${connectionQuotaEntity.metricName()} max connection creation rate to $quotaLimit");
        }
    }

    private MetricName connectionRateMetricName(ConnectionQuotaEntity connectionQuotaEntity) {
        return metrics.metricName(
            connectionQuotaEntity.metricName(),
            MetricsGroup,
            "Tracking rate of accepting new connections (per second)",
            connectionQuotaEntity.metricTags()
        );
    }

    private MetricConfig rateQuotaMetricConfig(int quotaLimit) {
        return new MetricConfig()
            .timeWindow(config.quotaConfig().quotaWindowSizeSeconds(), TimeUnit.SECONDS)
            .samples(config.quotaConfig().numQuotaSamples())
            .quota(new Quota(quotaLimit, true));
    }

    @Override
    public void close() {
        metrics.removeSensor(brokerConnectionRateSensor.name());
        for (ListenerConnectionQuota q : maxConnectionsPerListener.values()) {
            q.close();
        }
    }

    /**
     * Close {@code channel} and decrement the connection count.
     */
    public void closeChannel(ListenerName listenerName, SocketChannel channel) {
        if (channel != null) {
            LOGGER.debug("Closing connection from ${channel.socket().getRemoteSocketAddress()}");
            dec(listenerName, channel.socket().getInetAddress());
            closeSocket(channel);
        }
    }

    private static void closeSocket(SocketChannel ch) {
        try {
            ch.close();
        } catch (Exception ignore) {
        }
    }

    // =======================================================================
    // Inner class: ListenerConnectionQuota (mirrors Scala implementation)
    // =======================================================================

    public final class ListenerConnectionQuota implements ListenerReconfigurable, AutoCloseable {
        private final Object lock;
        private final ListenerName listener;

        volatile private int _maxConnections = Integer.MAX_VALUE;
        final Sensor connectionRateSensor = getOrCreateConnectionRateQuotaSensor(
            Integer.MAX_VALUE, ConnectionQuotaEntity.listenerQuotaEntity(listenerName().value())
        );
        final Sensor listenerConnectionRateThrottleSensor = createConnectionRateThrottleSensor(ConnectionQuotaEntity.LISTENER_THROTTLE_PREFIX);
        final Sensor ipConnectionRateThrottleSensor = createConnectionRateThrottleSensor(ConnectionQuotaEntity.IP_THROTTLE_PREFIX);

        ListenerConnectionQuota(Object lock, ListenerName listener) {
            this.lock = lock;
            this.listener = listener;
        }

        int maxConnections() { return _maxConnections; }

        @Override
        public ListenerName listenerName() { return listener; }

        @Override
        public void configure(Map<String, ?> configs) {
            _maxConnections = maxConnections(configs);
            updateConnectionRateQuota(maxConnectionCreationRate(configs), ConnectionQuotaEntity.listenerQuotaEntity(listener.value()));
        }

        @Override
        public Set<String> reconfigurableConfigs() {
            return SocketServer.ListenerReconfigurableConfigs();
        }

        @Override
        public void validateReconfiguration(Map<String, ?> configs) {
            int value = maxConnections(configs);
            if (value <= 0) throw new ConfigException("Invalid " + SocketServerConfigs.MAX_CONNECTIONS_CONFIG + " " + value);

            int rate = maxConnectionCreationRate(configs);
            if (rate <= 0) throw new ConfigException("Invalid " + SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG + " " + rate);
        }

        @Override
        public void reconfigure(Map<String, ?> configs) {
            synchronized (lock) {
                _maxConnections = maxConnections(configs);
                updateConnectionRateQuota(maxConnectionCreationRate(configs), ConnectionQuotaEntity.listenerQuotaEntity(listener.value()));
                lock.notifyAll();
            }
        }

        @Override
        public void close() {
            metrics.removeSensor(connectionRateSensor.name());
            metrics.removeSensor(listenerConnectionRateThrottleSensor.name());
            metrics.removeSensor(ipConnectionRateThrottleSensor.name());
        }

        private int maxConnections(Map<String, ?> configs) {
            Object v = configs.get(SocketServerConfigs.MAX_CONNECTIONS_CONFIG);
            return (v == null) ? Integer.MAX_VALUE : Integer.parseInt(v.toString());
        }

        private int maxConnectionCreationRate(Map<String, ?> configs) {
            Object v = configs.get(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG);
            return (v == null) ? Integer.MAX_VALUE : Integer.parseInt(v.toString());
        }

        /**
         * Creates sensor for tracking the average throttle time on this listener due to:
         * - hitting broker/listener connection rate quota
         * - IP connection rate quota
         * Average is over non-zero throttle times.
         */
        private Sensor createConnectionRateThrottleSensor(String throttlePrefix) {
            Sensor sensor = metrics.sensor(throttlePrefix + "ConnectionRateThrottleTime-" + listener.value());
            MetricName metricName = metrics.metricName(
                throttlePrefix + "connection-accept-throttle-time",
                MetricsGroup,
                "Tracking average throttle-time, out of non-zero throttle times, per listener",
                java.util.Map.of(ListenerMetricTag, listener.value())
            );
            sensor.add(metricName, new Avg());
            return sensor;
        }
    }
}