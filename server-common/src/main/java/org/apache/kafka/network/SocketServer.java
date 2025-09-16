/*
 * Licensed to the Apache Software Foundation (ASF) ...
 */

package org.apache.kafka.network;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.memory.SimpleMemoryPool;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.ServerConnectionId;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.config.BrokerReconfigurable;
import org.apache.kafka.security.CredentialProvider;
import org.apache.kafka.server.ApiVersionManager;
import org.apache.kafka.server.KafkaConfig;
import org.apache.kafka.server.ServerSocketFactory;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.network.ConnectionDisconnectListener;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.utils.CoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.message.ApiMessageType.ListenerType;
import static org.apache.kafka.network.SocketServer.Reconfigs.ReconfigurableConfigs;
import static org.apache.kafka.network.SocketServer.SocketServerStatics.MetricsGroup;

/**
 * Java refactor of the Scala SocketServer (data-plane).
 */
public class SocketServer implements BrokerReconfigurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SocketServer.class);

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

    private final KafkaConfig config;
    private final Metrics metrics;
    private final Time time;
    private final CredentialProvider credentialProvider;
    private final ApiVersionManager apiVersionManager;
    private final ServerSocketFactory socketFactory;
    private final List<ConnectionDisconnectListener> connectionDisconnectListeners;

    private final int maxQueuedRequests;
    protected final int nodeId;

    private final LogContext logContext;

    private final Sensor memoryPoolSensor;
    private final MemoryPool memoryPool;

    // data-plane
    final ConcurrentHashMap<Endpoint, DataPlaneAcceptor> dataPlaneAcceptors = new ConcurrentHashMap<>();
    final RequestChannel dataPlaneRequestChannel;

    private final AtomicInteger nextProcessorId = new AtomicInteger(0);
    final ConnectionQuotas connectionQuotas;

    /** Completed once all authorizer futures are complete. */
    private final CompletableFuture<Void> allAuthorizerFuturesComplete = new CompletableFuture<>();

    /** Guarded by this; true if stopProcessingRequests() called. */
    private boolean stopped = false;

    public SocketServer(KafkaConfig config,
                        Metrics metrics,
                        Time time,
                        CredentialProvider credentialProvider,
                        ApiVersionManager apiVersionManager) {
        this(config, metrics, time, credentialProvider, apiVersionManager, ServerSocketFactory.INSTANCE, List.of());
    }

    public SocketServer(KafkaConfig config,
                        Metrics metrics,
                        Time time,
                        CredentialProvider credentialProvider,
                        ApiVersionManager apiVersionManager,
                        ServerSocketFactory socketFactory,
                        List<ConnectionDisconnectListener> connectionDisconnectListeners) {

        this.config = config;
        this.metrics = metrics;
        this.time = time;
        this.credentialProvider = credentialProvider;
        this.apiVersionManager = apiVersionManager;
        this.socketFactory = socketFactory;
        this.connectionDisconnectListeners = connectionDisconnectListeners == null ? List.of() : connectionDisconnectListeners;

        this.maxQueuedRequests = config.queuedMaxRequests();
        this.nodeId = config.brokerId();
        this.logContext = new LogContext("[SocketServer listenerType=" + apiVersionManager.listenerType() + ", nodeId=" + nodeId + "] ");
        this.logIdent_$eq(logContext.logPrefix());

        this.memoryPoolSensor = metrics.sensor("MemoryPoolUtilization");
        MetricName memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup);
        MetricName memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup);
        memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName));

        this.memoryPool = (config.queuedMaxBytes() > 0)
            ? new SimpleMemoryPool(config.queuedMaxBytes(), config.socketRequestMaxBytes(), false, memoryPoolSensor)
            : MemoryPool.NONE;

        this.dataPlaneRequestChannel = new RequestChannel(maxQueuedRequests, time, apiVersionManager.newRequestMetrics());
        this.connectionQuotas = new ConnectionQuotas(config, time, metrics);

        // Gauges
        metricsGroup.newGauge("NetworkProcessorAvgIdlePercent", () -> {
            Collection<DataPlaneAcceptor> acceptors = dataPlaneAcceptors.values();
            List<Processor> procs = new ArrayList<>();
            for (DataPlaneAcceptor a : acceptors) procs.addAll(a.processors);
            if (procs.isEmpty()) return 1.0;
            double sum = 0.0;
            for (Processor p : procs) {
                MetricName nm = metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags);
                KafkaMetric m = metrics.metric(nm);
                sum += (m == null) ? 0.0 : Math.min((Double) m.metricValue(), 1.0);
            }
            return sum / procs.size();
        });

        metricsGroup.newGauge("MemoryPoolAvailable", () -> memoryPool.availableMemory());
        metricsGroup.newGauge("MemoryPoolUsed", () -> memoryPool.size() - memoryPool.availableMemory());

        metricsGroup.newGauge("ExpiredConnectionsKilledCount", () -> {
            Collection<DataPlaneAcceptor> acceptors = dataPlaneAcceptors.values();
            double sum = 0.0;
            for (DataPlaneAcceptor a : acceptors) {
                for (Processor p : a.processors) {
                    MetricName nm = metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags);
                    KafkaMetric m = metrics.metric(nm);
                    sum += (m == null) ? 0.0 : (Double) m.metricValue();
                }
            }
            return sum;
        });

        // Create acceptors/processors for configured endpoints (not started yet)
        if (apiVersionManager.listenerType() == ListenerType.CONTROLLER) {
            for (Endpoint e : config.controllerListeners()) createDataPlaneAcceptorAndProcessors(e);
        } else {
            for (Endpoint e : config.dataPlaneListeners()) createDataPlaneAcceptorAndProcessors(e);
        }
    }

    public int nextProcessorId() {
        return nextProcessorId.getAndIncrement();
    }

    /**
     * Enable request processing; acceptors will start after their authorizer futures complete.
     */
    public synchronized CompletableFuture<Void> enableRequestProcessing(Map<Endpoint, CompletableFuture<Void>> authorizerFutures) {
        if (stopped) throw new RuntimeException("Can't enable request processing: SocketServer is stopped.");

        LOGGER.info("Enabling request processing.");

        for (DataPlaneAcceptor acceptor : dataPlaneAcceptors.values()) {
            CompletableFuture<Void> f = allAuthorizerFuturesComplete;
            for (Map.Entry<Endpoint, CompletableFuture<Void>> e : authorizerFutures.entrySet()) {
                if (acceptor.endPoint.listener().equals(e.getKey().listener())) {
                    f = e.getValue();
                    break;
                }
            }
            f.whenComplete((__, ex) -> {
                if (ex != null) {
                    acceptor.startedFuture.completeExceptionally(ex);
                } else {
                    acceptor.start();
                }
            });
        }

        FutureUtils.chainFuture(CompletableFuture.allOf(authorizerFutures.values().toArray(CompletableFuture[]::new)), allAuthorizerFuturesComplete);

        CompletableFuture<Void> enableFuture = new CompletableFuture<>();
        CompletableFuture<?>[] arr = dataPlaneAcceptors.values().stream().map(a -> a.startedFuture).toArray(CompletableFuture[]::new);
        FutureUtils.chainFuture(CompletableFuture.allOf(arr), enableFuture);
        return enableFuture;
    }

    private synchronized void createDataPlaneAcceptorAndProcessors(Endpoint endpoint) {
        if (stopped) throw new RuntimeException("Can't create new data plane acceptor and processors: SocketServer is stopped.");
        ListenerName listenerName = ListenerName.normalised(endpoint.listener());
        Map<String, Object> parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix());
        connectionQuotas.addListener(config, listenerName);
        boolean isPrivilegedListener = config.interBrokerListenerName().equals(listenerName);

        DataPlaneAcceptor acceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel);
        config.addReconfigurable(acceptor);
        acceptor.configure(parsedConfigs);
        dataPlaneAcceptors.put(endpoint, acceptor);
        LOGGER.info("Created data-plane acceptor and processors for endpoint : " + listenerName);
    }

    private Map<ListenerName, Endpoint> endpoints() {
        Map<ListenerName, Endpoint> m = new HashMap<>();
        for (Endpoint e : config.listeners()) {
            m.put(ListenerName.normalised(e.listener()), e);
        }
        return m;
    }

    protected DataPlaneAcceptor createDataPlaneAcceptor(Endpoint endPoint,
                                                        boolean isPrivilegedListener,
                                                        RequestChannel requestChannel) {
        return new DataPlaneAcceptor(this, endPoint, config, nodeId, connectionQuotas, time, isPrivilegedListener,
            requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager);
    }

    /** Stop processing requests and new connections. */
    public synchronized void stopProcessingRequests() {
        if (!stopped) {
            stopped = true;
            LOGGER.info("Stopping socket server request processors");
            for (DataPlaneAcceptor a : dataPlaneAcceptors.values()) a.beginShutdown();
            for (DataPlaneAcceptor a : dataPlaneAcceptors.values()) a.close();
            dataPlaneRequestChannel.clear();
            LOGGER.info("Stopped socket server request processors");
        }
    }

    /** Full shutdown. */
    public void shutdown() {
        LOGGER.info("Shutting down socket server");
        allAuthorizerFuturesComplete.completeExceptionally(
            new TimeoutException("The socket server was shut down before the Authorizer could be completely initialized."));
        synchronized (this) {
            stopProcessingRequests();
            dataPlaneRequestChannel.shutdown();
            connectionQuotas.close();
        }
        LOGGER.info("Shutdown completed");
    }

    public int boundPort(ListenerName listenerName) {
        try {
            Endpoint ep = endpoints().get(listenerName);
            DataPlaneAcceptor acceptor = (ep == null) ? null : dataPlaneAcceptors.get(ep);
            if (acceptor != null) return acceptor.localPort;
            throw new KafkaException("Could not find listenerName : " + listenerName + " in data-plane.");
        } catch (Exception e) {
            throw new KafkaException("Tried to check for port of non-existing protocol", e);
        }
    }

    /** Dynamically add listeners. */
    public synchronized void addListeners(List<Endpoint> listenersAdded) {
        if (stopped) throw new RuntimeException("can't add new listeners: SocketServer is stopped.");
        LOGGER.info("Adding data-plane listeners for endpoints " + listenersAdded);
        for (Endpoint endpoint : listenersAdded) {
            createDataPlaneAcceptorAndProcessors(endpoint);
            DataPlaneAcceptor acceptor = dataPlaneAcceptors.get(endpoint);
            allAuthorizerFuturesComplete.whenComplete((__, ex) -> {
                if (ex != null) acceptor.startedFuture.completeExceptionally(ex);
                else acceptor.start();
            });
        }
    }

    public synchronized void removeListeners(List<Endpoint> listenersRemoved) {
        LOGGER.info("Removing data-plane listeners for endpoints " + listenersRemoved);
        for (Endpoint endpoint : listenersRemoved) {
            connectionQuotas.removeListener(config, ListenerName.normalised(endpoint.listener()));
            DataPlaneAcceptor acceptor = dataPlaneAcceptors.remove(endpoint);
            if (acceptor != null) {
                acceptor.beginShutdown();
                acceptor.close();
                config.removeReconfigurable(acceptor);
            }
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return ReconfigurableConfigs;
    }

    @Override
    public void validateReconfiguration(KafkaConfig newConfig) {
        // no-op (matches Scala)
    }

    @Override
    public void reconfigure(KafkaConfig oldConfig, KafkaConfig newConfig) {
        int maxConnectionsPerIp = newConfig.maxConnectionsPerIp();
        if (maxConnectionsPerIp != oldConfig.maxConnectionsPerIp()) {
            LOGGER.info("Updating maxConnectionsPerIp: " + maxConnectionsPerIp);
            connectionQuotas.updateMaxConnectionsPerIp(maxConnectionsPerIp);
        }

        Map<String, Integer> maxConnectionsPerIpOverrides = newConfig.maxConnectionsPerIpOverrides();
        if (!Objects.equals(maxConnectionsPerIpOverrides, oldConfig.maxConnectionsPerIpOverrides())) {
            LOGGER.info("Updating maxConnectionsPerIpOverrides: " + stringifyOverrides(maxConnectionsPerIpOverrides));
            connectionQuotas.updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides);
        }

        int maxConnections = newConfig.maxConnections();
        if (maxConnections != oldConfig.maxConnections()) {
            LOGGER.info("Updating broker-wide maxConnections: " + maxConnections);
            connectionQuotas.updateBrokerMaxConnections(maxConnections);
        }

        int maxConnectionRate = newConfig.maxConnectionCreationRate();
        if (maxConnectionRate != oldConfig.maxConnectionCreationRate()) {
            LOGGER.info("Updating broker-wide maxConnectionCreationRate: " + maxConnectionRate);
            connectionQuotas.updateBrokerMaxConnectionRate(maxConnectionRate);
        }
    }

    private static String stringifyOverrides(Map<String, Integer> m) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Integer> e : m.entrySet()) {
            if (!first) sb.append(',');
            sb.append(e.getKey()).append('=').append(e.getValue());
            first = false;
        }
        return sb.toString();
    }

    // For test usage
    int connectionCount(InetAddress address) {
        return (connectionQuotas == null) ? 0 : connectionQuotas.get(address);
    }

    // For test usage
    Optional<DataPlaneAcceptor> dataPlaneAcceptor(String listenerName) {
        for (Map.Entry<Endpoint, DataPlaneAcceptor> e : dataPlaneAcceptors.entrySet()) {
            if (e.getKey().listener().equals(listenerName)) return Optional.of(e.getValue());
        }
        return Optional.empty();
    }

    /** ===== static companions / constants ===== */
    public static final class SocketServerStatics {
        public static final String MetricsGroup = "socket-server-metrics";
    }

    public static final class Reconfigs {
        public static final Set<String> ReconfigurableConfigs = Set.of(
            SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG,
            SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG,
            SocketServerConfigs.MAX_CONNECTIONS_CONFIG,
            SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG
        );

        public static final Set<String> ListenerReconfigurableConfigs =
            Set.of(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG);
    }

    public static final class DataPlaneAcceptorStatics {
        public static final Set<String> ListenerReconfigurableConfigs =
            Set.of(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG);
    }

    public static void closeSocket(SocketChannel channel) {
        Utils.closeQuietly(channel.socket(), "channel socket");
        Utils.closeQuietly(channel, "channel");
    }

    /** ======================= DataPlaneAcceptor ======================= */

    public static final class DataPlaneAcceptor extends Acceptor implements ListenerReconfigurable {

        DataPlaneAcceptor(SocketServer socketServer,
                          Endpoint endPoint,
                          KafkaConfig config,
                          int nodeId,
                          ConnectionQuotas connectionQuotas,
                          Time time,
                          boolean isPrivilegedListener,
                          RequestChannel requestChannel,
                          Metrics metrics,
                          CredentialProvider credentialProvider,
                          LogContext logContext,
                          MemoryPool memoryPool,
                          ApiVersionManager apiVersionManager) {
            super(socketServer, endPoint, config, nodeId, connectionQuotas, time, isPrivilegedListener,
                requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager);
        }

        @Override
        public ListenerName listenerName() {
            return ListenerName.normalised(endPoint.listener());
        }

        @Override
        public Set<String> reconfigurableConfigs() {
            return DataPlaneAcceptorStatics.ListenerReconfigurableConfigs;
        }

        @Override
        public void validateReconfiguration(Map<String, ?> configs) {
            for (Map.Entry<String, ?> e : configs.entrySet()) {
                if (reconfigurableConfigs().contains(e.getKey())) {
                    int newValue = Integer.parseInt(e.getValue().toString());
                    int oldValue = processors.size();
                    if (newValue != oldValue) {
                        String errorMsg = "Dynamic thread count update validation failed for " + e.getKey() + "=" + e.getValue();
                        if (newValue <= 0) throw new ConfigException(errorMsg + ", value should be at least 1");
                        if (newValue < oldValue / 2) throw new ConfigException(errorMsg + ", value should be at least half the current value " + oldValue);
                        if (newValue > oldValue * 2) throw new ConfigException(errorMsg + ", value should not be greater than double the current value " + oldValue);
                    }
                }
            }
        }

        @Override
        public void reconfigure(Map<String, ?> configs) {
            int newNumNetworkThreads = Integer.parseInt(configs.get(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG).toString());
            if (newNumNetworkThreads != processors.size()) {
                LOGGER.info("Resizing network thread pool size for " + endPoint.listener() + " listener from " + processors.size() + " to " + newNumNetworkThreads);
                if (newNumNetworkThreads > processors.size()) addProcessors(newNumNetworkThreads - processors.size());
                else removeProcessors(processors.size() - newNumNetworkThreads);
            }
        }

        @Override
        public void configure(Map<String, ?> configs) {
            addProcessors(Integer.parseInt(configs.get(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG).toString()));
        }
    }

    /** ======================= Acceptor (abstract) ======================= */

    static abstract class Acceptor implements Runnable {
        private static final Logger LOGGER = LoggerFactory.getLogger(Acceptor.class);

        protected final SocketServer socketServer;
        protected final Endpoint endPoint;
        protected KafkaConfig config;
        protected final ConnectionQuotas connectionQuotas;
        protected final Time time;
        protected final boolean isPrivilegedListener;
        protected final RequestChannel requestChannel;
        protected final Metrics metrics;
        protected final CredentialProvider credentialProvider;
        protected final LogContext logContext;
        protected final MemoryPool memoryPool;
        protected final ApiVersionManager apiVersionManager;

        protected final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

        final AtomicBoolean shouldRun = new AtomicBoolean(true);

        private final int sendBufferSize;
        private final int recvBufferSize;
        private final int listenBacklogSize;

        private final Selector nioSelector;

        protected ServerSocketChannel serverChannel;
        protected final int localPort;

        protected final List<Processor> processors = new ArrayList<>();

        private final KafkaMetricsGroup backwardCompatibilityMetricGroup = new KafkaMetricsGroup("kafka.network", "Acceptor");
        private final MetricName blockedPercentMeterMetricName;
        private final com.yammer.metrics.core.Meter blockedPercentMeter;

        private int currentProcessorIndex = 0;
        protected final PriorityQueue<DelayedCloseSocket> throttledSockets = new PriorityQueue<>();
        private final AtomicBoolean started = new AtomicBoolean();
        final CompletableFuture<Void> startedFuture = new CompletableFuture<>();

        final KafkaThread thread;

        protected Acceptor(SocketServer socketServer,
                           Endpoint endPoint,
                           KafkaConfig config,
                           int nodeId,
                           ConnectionQuotas connectionQuotas,
                           Time time,
                           boolean isPrivilegedListener,
                           RequestChannel requestChannel,
                           Metrics metrics,
                           CredentialProvider credentialProvider,
                           LogContext logContext,
                           MemoryPool memoryPool,
                           ApiVersionManager apiVersionManager) {
            this.socketServer = socketServer;
            this.endPoint = endPoint;
            this.config = config;
            this.connectionQuotas = connectionQuotas;
            this.time = time;
            this.isPrivilegedListener = isPrivilegedListener;
            this.requestChannel = requestChannel;
            this.metrics = metrics;
            this.credentialProvider = credentialProvider;
            this.logContext = logContext;
            this.memoryPool = memoryPool;
            this.apiVersionManager = apiVersionManager;

            this.sendBufferSize = config.socketSendBufferBytes();
            this.recvBufferSize = config.socketReceiveBufferBytes();
            this.listenBacklogSize = config.socketListenBacklogSize();

            try {
                this.nioSelector = Selector.open();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (endPoint.port() != 0) {
                this.localPort = endPoint.port();
            } else {
                this.serverChannel = openServerSocket(endPoint.host(), endPoint.port(), listenBacklogSize);
                int p = this.serverChannel.socket().getLocalPort();
                LOGGER.info("Opened wildcard endpoint {}:{}", endPoint.host(), p);
                this.localPort = p;
            }

            Map<String, String> mtags = Map.of(Processor.ListenerMetricTag, endPoint.listener());
            this.blockedPercentMeterMetricName = backwardCompatibilityMetricGroup.metricName("AcceptorBlockedPercent", mtags);
            this.blockedPercentMeter = metricsGroup.newMeter(blockedPercentMeterMetricName, "blocked time", TimeUnit.NANOSECONDS);

            this.thread = KafkaThread.nonDaemon(
                "data-plane-kafka-socket-acceptor-" + endPoint.listener() + "-" + endPoint.securityProtocol() + "-" + endPoint.port(),
                this
            );
        }

        public synchronized void start() {
            try {
                if (!shouldRun.get()) throw new ClosedChannelException();
                if (serverChannel == null) {
                    serverChannel = openServerSocket(endPoint.host(), endPoint.port(), listenBacklogSize);
                    LOGGER.debug("Opened endpoint {}:{}", endPoint.host(), endPoint.port());
                }
                LOGGER.debug("Starting processors for listener {}", endPoint.listener());
                for (Processor p : processors) p.start();
                LOGGER.debug("Starting acceptor thread for listener {}", endPoint.listener());
                thread.start();
                startedFuture.complete(null);
                started.set(true);
            } catch (ClosedChannelException e) {
                LOGGER.debug("Refusing to start acceptor for {} since the acceptor has already been shut down.", endPoint.listener());
                startedFuture.completeExceptionally(e);
            } catch (Throwable t) {
                LOGGER.error("Unable to start acceptor for {}", endPoint.listener(), t);
                startedFuture.completeExceptionally(new RuntimeException("Unable to start acceptor for " + endPoint.listener(), t));
            }
        }

        static final class DelayedCloseSocket implements Comparable<DelayedCloseSocket> {
            final SocketChannel socket;
            final long endThrottleTimeMs;
            DelayedCloseSocket(SocketChannel socket, long endThrottleTimeMs) {
                this.socket = socket;
                this.endThrottleTimeMs = endThrottleTimeMs;
            }
            @Override public int compareTo(DelayedCloseSocket o) {
                return Long.compare(this.endThrottleTimeMs, o.endThrottleTimeMs);
            }
        }

        protected synchronized void removeProcessors(int removeCount) {
            int size = processors.size();
            List<Processor> toRemove = new ArrayList<>(processors.subList(size - removeCount, size));
            processors.subList(size - removeCount, size).clear();
            for (Processor p : toRemove) p.close();
            for (Processor p : toRemove) requestChannel.removeProcessor(p.id);
        }

        public void beginShutdown() {
            if (shouldRun.getAndSet(false)) {
                wakeup();
                synchronized (this) {
                    for (Processor p : processors) p.beginShutdown();
                }
            }
        }

        public void close() {
            beginShutdown();
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!started.get()) closeAll();
            synchronized (this) {
                for (Processor p : processors) p.close();
            }
        }

        @Override
        public void run() {
            try {
                serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
                while (shouldRun.get()) {
                    try {
                        acceptNewConnections();
                        closeThrottledConnections();
                    } catch (Throwable e) {
                        if (e instanceof ThreadDeath) throw e;
                        error("Error occurred", e);
                    }
                }
            } catch (IOException e) {
                error("Selector registration failed", e);
            } finally {
                closeAll();
            }
        }

        private void closeAll() {
            LOGGER.debug("Closing server socket, selector, and any throttled sockets.");
            Utils.closeQuietly(serverChannel, "Acceptor serverChannel");
            Utils.closeQuietly(nioSelector, "Acceptor nioSelector");
            while (!throttledSockets.isEmpty()) {
                DelayedCloseSocket s = throttledSockets.poll();
                if (s != null) closeSocket(s.socket);
            }
        }

        private ServerSocketChannel openServerSocket(String host, int port, int backlog) {
            InetSocketAddress socketAddress = Utils.isBlank(host) ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
            ServerSocketChannel ch = socketServer.socketFactory.openServerSocket(endPoint.listener(), socketAddress, backlog, recvBufferSize);
            LOGGER.info("Awaiting socket connections on " + socketAddress.getHostString() + ":" + ch.socket().getLocalPort() + ".");
            return ch;
        }

        private void acceptNewConnections() throws IOException {
            int ready = nioSelector.select(500);
            if (ready > 0) {
                Set<SelectionKey> keys = nioSelector.selectedKeys();
                Iterator<SelectionKey> iter = keys.iterator();
                while (iter.hasNext() && shouldRun.get()) {
                    try {
                        SelectionKey key = iter.next();
                        iter.remove();
                        if (key.isAcceptable()) {
                            Optional<SocketChannel> accepted = accept(key);
                            if (accepted.isPresent()) {
                                SocketChannel socketChannel = accepted.get();
                                int retriesLeft = processors.size();
                                Processor processor = null;
                                do {
                                    retriesLeft -= 1;
                                    synchronized (this) {
                                        if (processors.isEmpty()) throw new IllegalStateException("No processors available");
                                        currentProcessorIndex = currentProcessorIndex % processors.size();
                                        processor = processors.get(currentProcessorIndex);
                                    }
                                    currentProcessorIndex += 1;
                                } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0));
                            }
                        } else {
                            throw new IllegalStateException("Unrecognized key state for acceptor thread.");
                        }
                    } catch (Throwable e) {
                        LOGGER.error("Error while accepting connection", e);
                    }
                }
            }
        }

        private Optional<SocketChannel> accept(SelectionKey key) throws IOException {
            ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
            SocketChannel ch = ssc.accept();
            ListenerName listenerName = ListenerName.normalised(endPoint.listener());
            try {
                connectionQuotas.inc(listenerName, ch.socket().getInetAddress(), blockedPercentMeter);
                configureAcceptedSocketChannel(ch);
                return Optional.of(ch);
            } catch (TooManyConnectionsException e) {
                LOGGER.info("Rejected connection from {}, address already has the configured maximum of {} connections.", e.ip, e.count);
                connectionQuotas.closeChannel(listenerName, ch);
                return Optional.empty();
            } catch (ConnectionThrottledException e) {
                InetAddress ip = ch.socket().getInetAddress();
                LOGGER.debug("Delaying closing of connection from {} for {} ms", ip, e.throttleTimeMs);
                long endThrottleTimeMs = e.startThrottleTimeMs + e.throttleTimeMs;
                throttledSockets.add(new DelayedCloseSocket(ch, endThrottleTimeMs));
                return Optional.empty();
            } catch (IOException e) {
                LOGGER.error("Encountered an error while configuring the connection, closing it.", e);
                connectionQuotas.closeChannel(listenerName, ch);
                return Optional.empty();
            }
        }

        protected void configureAcceptedSocketChannel(SocketChannel ch) throws IOException {
            ch.configureBlocking(false);
            ch.socket().setTcpNoDelay(true);
            ch.socket().setKeepAlive(true);
            if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) ch.socket().setSendBufferSize(sendBufferSize);
        }

        private void closeThrottledConnections() throws IOException {
            long now = time.milliseconds();
            while (!throttledSockets.isEmpty() && throttledSockets.peek().endThrottleTimeMs < now) {
                DelayedCloseSocket closing = throttledSockets.poll();
                if (closing != null) {
                    LOGGER.debug("Closing socket from ip {}", closing.socket.getRemoteAddress());
                    closeSocket(closing.socket);
                }
            }
        }

        private boolean assignNewConnection(SocketChannel ch, Processor processor, boolean mayBlock) throws SocketException {
            if (processor.accept(ch, mayBlock, blockedPercentMeter)) {
                LOGGER.debug("Accepted connection from {} on {} and assigned it to processor {}, " +
                    "sendBufferSize [actual|requested]: [{}|{}] recvBufferSize [actual|requested]: [{}|{}]",
                    ch.socket().getRemoteSocketAddress(), ch.socket().getLocalSocketAddress(), processor.id,
                    ch.socket().getSendBufferSize(), sendBufferSize, ch.socket().getReceiveBufferSize(), recvBufferSize);
                return true;
            }
            return false;
        }

        public void wakeup() { nioSelector.wakeup(); }

        public synchronized void addProcessors(int toCreate) {
            ListenerName ln = ListenerName.normalised(endPoint.listener());
            SecurityProtocol sp = endPoint.securityProtocol();
            List<Processor> listenerProcessors = new ArrayList<>();
            for (int i = 0; i < toCreate; i++) {
                Processor p = newProcessor(socketServer.nextProcessorId(), ln, sp, socketServer.connectionDisconnectListeners);
                listenerProcessors.add(p);
                requestChannel.addProcessor(p);
                if (started.get()) p.start();
            }
            processors.addAll(listenerProcessors);
        }

        Processor newProcessor(int id,
                               ListenerName listenerName,
                               SecurityProtocol securityProtocol,
                               List<ConnectionDisconnectListener> disconnectListeners) {
            String name = "data-plane-kafka-network-thread-" + socketServer.nodeId + "-" + endPoint.listener() + "-" + endPoint.securityProtocol() + "-" + id;
            return new Processor(id, time, config.socketRequestMaxBytes(), requestChannel, connectionQuotas, config.connectionsMaxIdleMs(),
                config.failedAuthenticationDelayMs(), listenerName, securityProtocol, config, metrics, credentialProvider, memoryPool,
                logContext, Processor.ConnectionQueueSize, isPrivilegedListener, apiVersionManager, name, disconnectListeners);
        }
    }

    /** ======================= Processor ======================= */

    static final class Processor implements Runnable {
        private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);

        private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

        static final String IdlePercentMetricName = "IdlePercent";
        static final String NetworkProcessorMetricTag = "networkProcessor";
        static final String ListenerMetricTag = "listener";
        static final int ConnectionQueueSize = 20;

        static RequestHeader parseRequestHeader(ApiVersionManager apiVersionManager, ByteBuffer buffer) {
            RequestHeader header = RequestHeader.parse(buffer);
            if (apiVersionManager.isApiEnabled(header.apiKey(), header.apiVersion())) return header;
            if (header.isApiVersionSupported()) {
                throw new InvalidRequestException("Received request for disabled api with key " + header.apiKey().id +
                    " (" + header.apiKey().name + ") and version " + header.apiVersion());
            }
            throw new UnsupportedVersionException("Received request for api with key " + header.apiKey().id +
                " (" + header.apiKey().name + ") and unsupported version " + header.apiVersion());
        }

        final int id;
        private final Time time;
        private final int maxRequestSize;
        private final RequestChannel requestChannel;
        private final ConnectionQuotas connectionQuotas;
        private final long connectionsMaxIdleMs;
        private final int failedAuthenticationDelayMs;
        private final ListenerName listenerName;
        private final SecurityProtocol securityProtocol;
        private final KafkaConfig config;
        private final Metrics metrics;
        private final CredentialProvider credentialProvider;
        private final MemoryPool memoryPool;
        private final LogContext logContext;
        private final int connectionQueueSize;
        private final boolean isPrivilegedListener;
        private final ApiVersionManager apiVersionManager;
        private final String threadName;
        private final List<ConnectionDisconnectListener> connectionDisconnectListeners;

        final AtomicBoolean shouldRun = new AtomicBoolean(true);
        private final AtomicBoolean started = new AtomicBoolean();
        final KafkaThread thread;

        private final ArrayBlockingQueue<SocketChannel> newConnections;
        private final Map<String, RequestChannel.Response> inflightResponses = new ConcurrentHashMap<>();
        private final LinkedBlockingDeque<RequestChannel.Response> responseQueue = new LinkedBlockingDeque<>();

        final Map<String, String> metricTags;

        private final CumulativeSum expiredConnectionsKilledCount = new CumulativeSum();
        private final MetricName expiredConnectionsKilledCountMetricName;

        final Selector selector;

        private int nextConnectionIndex = 0;

        Processor(int id,
                  Time time,
                  int maxRequestSize,
                  RequestChannel requestChannel,
                  ConnectionQuotas connectionQuotas,
                  long connectionsMaxIdleMs,
                  int failedAuthenticationDelayMs,
                  ListenerName listenerName,
                  SecurityProtocol securityProtocol,
                  KafkaConfig config,
                  Metrics metrics,
                  CredentialProvider credentialProvider,
                  MemoryPool memoryPool,
                  LogContext logContext,
                  int connectionQueueSize,
                  boolean isPrivilegedListener,
                  ApiVersionManager apiVersionManager,
                  String threadName,
                  List<ConnectionDisconnectListener> connectionDisconnectListeners) {

            this.id = id;
            this.time = time;
            this.maxRequestSize = maxRequestSize;
            this.requestChannel = requestChannel;
            this.connectionQuotas = connectionQuotas;
            this.connectionsMaxIdleMs = connectionsMaxIdleMs;
            this.failedAuthenticationDelayMs = failedAuthenticationDelayMs;
            this.listenerName = listenerName;
            this.securityProtocol = securityProtocol;
            this.config = config;
            this.metrics = metrics;
            this.credentialProvider = credentialProvider;
            this.memoryPool = memoryPool;
            this.logContext = logContext;
            this.connectionQueueSize = connectionQueueSize;
            this.isPrivilegedListener = isPrivilegedListener;
            this.apiVersionManager = apiVersionManager;
            this.threadName = threadName;
            this.connectionDisconnectListeners = connectionDisconnectListeners;

            this.thread = KafkaThread.nonDaemon(threadName, this);
            this.newConnections = new ArrayBlockingQueue<>(connectionQueueSize);

            Map<String, String> tags = new LinkedHashMap<>();
            tags.put(ListenerMetricTag, listenerName.value());
            tags.put(NetworkProcessorMetricTag, Integer.toString(id));
            this.metricTags = Collections.unmodifiableMap(tags);

            metricsGroup.newGauge(IdlePercentMetricName, () -> {
                KafkaMetric m = (KafkaMetric) metrics.metric(metrics.metricName("io-wait-ratio", MetricsGroup, metricTags));
                return (m == null) ? 0.0 : Math.min((Double) m.metricValue(), 1.0);
            }, Map.of(NetworkProcessorMetricTag, Integer.toString(id)));

            this.expiredConnectionsKilledCountMetricName = metrics.metricName("expired-connections-killed-count", MetricsGroup, metricTags);
            metrics.addMetric(expiredConnectionsKilledCountMetricName, expiredConnectionsKilledCount);

            ChannelBuilder channelBuilder = ChannelBuilders.serverChannelBuilder(
                listenerName,
                listenerName.equals(config.interBrokerListenerName()),
                securityProtocol,
                config,
                credentialProvider.credentialCache(),
                credentialProvider.tokenCache(),
                time,
                logContext,
                version -> apiVersionManager.apiVersionResponse(0, version < 4)
            );
            if (channelBuilder instanceof Reconfigurable reconfigurable) {
                config.addReconfigurable(reconfigurable);
            }
            this.selector = new Selector(
                maxRequestSize,
                connectionsMaxIdleMs,
                failedAuthenticationDelayMs,
                metrics,
                time,
                "socket-server",
                metricTags,
                false,
                true,
                channelBuilder,
                memoryPool,
                logContext
            );
        }

        @Override
        public void run() {
            try {
                while (shouldRun.get()) {
                    try {
                        configureNewConnections();
                        processNewResponses();
                        poll();
                        processCompletedReceives();
                        processCompletedSends();
                        processDisconnected();
                        closeExcessConnections();
                    } catch (Throwable e) {
                        LOGGER.error("Processor got uncaught exception.", e);
                    }
                }
            } finally {
                LOGGER.debug("Closing selector - processor " + id);
                CoreUtils.swallow(this::closeAll, LOGGER, Level.ERROR);
            }
        }

        void processException(String errorMessage, Throwable t) {
            LOGGER.error(errorMessage, t);
        }

        private void processChannelException(String channelId, String msg, Throwable t) {
            if (openOrClosingChannel(channelId).isPresent()) {
                LOGGER.error("Closing socket for " + channelId + " because of error", t);
                close(channelId);
            }
            processException(msg, t);
        }

        private void processNewResponses() {
            RequestChannel.Response current;
            while ((current = dequeueResponse()) != null) {
                String channelId = current.request.context.connectionId();
                try {
                    if (current instanceof RequestChannel.NoOpResponse) {
                        updateRequestMetrics(current);
                        LOGGER.trace("Socket server received empty response to send, registering for read: " + current);
                        handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT);
                        tryUnmuteChannel(channelId);
                    } else if (current instanceof RequestChannel.SendResponse sr) {
                        sendResponse(current, sr.responseSend);
                    } else if (current instanceof RequestChannel.CloseConnectionResponse) {
                        updateRequestMetrics(current);
                        LOGGER.trace("Closing socket connection actively according to the response code.");
                        close(channelId);
                    } else if (current instanceof RequestChannel.StartThrottlingResponse) {
                        handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED);
                    } else if (current instanceof RequestChannel.EndThrottlingResponse) {
                        handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED);
                        tryUnmuteChannel(channelId);
                    } else {
                        throw new IllegalArgumentException("Unknown response type: " + current.getClass());
                    }
                } catch (Throwable e) {
                    processChannelException(channelId, "Exception while processing response for " + channelId, e);
                }
            }
        }

        protected void sendResponse(RequestChannel.Response response, Send responseSend) {
            String connectionId = response.request.context.connectionId();
            LOGGER.trace("Socket server received response to send to " + connectionId + ", registering for write and sending data: " + response);
            if (channel(connectionId).isEmpty()) {
                LOGGER.warn("Attempting to send response via channel for which there is no open connection, connection id " + connectionId);
                response.request.updateRequestMetrics(0L, response);
            }
            if (openOrClosingChannel(connectionId).isPresent()) {
                selector.send(new NetworkSend(connectionId, responseSend));
                inflightResponses.put(connectionId, response);
            }
        }

        private void poll() {
            int pollTimeout = newConnections.isEmpty() ? 300 : 0;
            try {
                selector.poll(pollTimeout);
            } catch (IllegalStateException | IOException e) {
                LOGGER.error("Processor " + id + " poll failed", e);
            }
        }

        private void processCompletedReceives() {
            for (NetworkReceive receive : selector.completedReceives()) {
                try {
                    Optional<KafkaChannel> chOpt = openOrClosingChannel(receive.source());
                    if (chOpt.isPresent()) {
                        KafkaChannel channel = chOpt.get();
                        RequestHeader header = parseRequestHeader(apiVersionManager, receive.payload());

                        if (header.apiKey() == ApiKeys.SASL_HANDSHAKE &&
                            channel.maybeBeginServerReauthentication(receive, () -> time.nanoseconds())) {
                            LOGGER.trace("Begin re-authentication: " + channel);
                        } else {
                            long nowNanos = time.nanoseconds();
                            if (channel.serverAuthenticationSessionExpired(nowNanos)) {
                                LOGGER.debug("Disconnecting expired channel: " + channel + " : " + header);
                                close(channel.id());
                                expiredConnectionsKilledCount.record(null, 1, 0);
                            } else {
                                String connectionId = receive.source();
                                RequestContext ctx = new RequestContext(
                                    header,
                                    connectionId,
                                    channel.socketAddress(),
                                    Optional.of(channel.socketPort()),
                                    channel.principal(),
                                    listenerName,
                                    securityProtocol,
                                    channel.channelMetadataRegistry().clientInformation(),
                                    isPrivilegedListener,
                                    channel.principalSerde()
                                );

                                RequestChannel.Request req = new RequestChannel.Request(id, ctx, nowNanos, memoryPool, receive.payload(),
                                    requestChannel.metrics, Optional.empty());

                                if (header.apiKey() == ApiKeys.API_VERSIONS) {
                                    ApiVersionsRequest avr = req.body(ApiVersionsRequest.class);
                                    if (avr.isValid()) {
                                        channel.channelMetadataRegistry().registerClientInformation(
                                            new ClientInformation(avr.data().clientSoftwareName(), avr.data().clientSoftwareVersion()));
                                    }
                                }

                                requestChannel.sendRequest(req);
                                selector.mute(connectionId);
                                handleChannelMuteEvent(connectionId, ChannelMuteEvent.REQUEST_RECEIVED);
                            }
                        }
                    } else {
                        throw new IllegalStateException("Channel " + receive.source() + " removed from selector before processing completed receive");
                    }
                } catch (Throwable e) {
                    processChannelException(receive.source(), "Exception while processing request from " + receive.source(), e);
                }
            }
            selector.clearCompletedReceives();
        }

        private void processCompletedSends() {
            for (Send send : selector.completedSends()) {
                try {
                    RequestChannel.Response response = inflightResponses.remove(send.destinationId());
                    if (response == null) throw new IllegalStateException("Send for " + send.destinationId() + " completed, but not in inflightResponses");
                    response.onComplete.ifPresent(cb -> cb.accept(send));
                    updateRequestMetrics(response);
                    handleChannelMuteEvent(send.destinationId(), ChannelMuteEvent.RESPONSE_SENT);
                    tryUnmuteChannel(send.destinationId());
                } catch (Throwable e) {
                    processChannelException(send.destinationId(), "Exception while processing completed send to " + send.destinationId(), e);
                }
            }
            selector.clearCompletedSends();
        }

        private void updateRequestMetrics(RequestChannel.Response response) {
            RequestChannel.Request request = response.request;
            Optional<KafkaChannel> ch = openOrClosingChannel(request.context.connectionId());
            long networkThreadTimeNanos = ch.map(KafkaChannel::getAndResetNetworkThreadTimeNanos).orElse(0L);
            request.updateRequestMetrics(networkThreadTimeNanos, response);
        }

        private void processDisconnected() {
            for (String connectionId : selector.disconnected().keySet()) {
                try {
                    ServerConnectionId sci = ServerConnectionId.fromString(connectionId)
                        .orElseThrow(() -> new IllegalStateException("connectionId has unexpected format: " + connectionId));
                    String remoteHost = sci.remoteHost;
                    RequestChannel.Response r = inflightResponses.remove(connectionId);
                    if (r != null) updateRequestMetrics(r);
                    try {
                        connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost));
                    } catch (UnknownHostException ignored) { }
                    for (ConnectionDisconnectListener l : connectionDisconnectListeners) {
                        CoreUtils.swallow(() -> l.onDisconnect(connectionId), this, Level.ERROR);
                    }
                } catch (Throwable e) {
                    processException("Exception while processing disconnection of " + connectionId, e);
                }
            }
        }

        private void closeExcessConnections() throws IOException {
            if (connectionQuotas.maxConnectionsExceeded(listenerName)) {
                KafkaChannel channel = selector.lowestPriorityChannel();
                if (channel != null) close(channel.id());
            }
        }

        private void close(String connectionId) throws IOException {
            Optional<KafkaChannel> chOpt = openOrClosingChannel(connectionId);
            if (chOpt.isPresent()) {
                KafkaChannel channel = chOpt.get();
                LOGGER.debug("Closing selector connection " + connectionId);
                InetAddress address = channel.socketAddress();
                connectionQuotas.dec(listenerName, address);
                selector.close();
                for (ConnectionDisconnectListener l : connectionDisconnectListeners) {
                    CoreUtils.swallow(() -> l.onDisconnect(connectionId), this, Level.ERROR);
                }
                RequestChannel.Response r = inflightResponses.remove(connectionId);
                if (r != null) updateRequestMetrics(r);
            }
        }

        public boolean accept(SocketChannel ch, boolean mayBlock, com.yammer.metrics.core.Meter acceptorBlockedPercentMeter) {
            boolean accepted;
            if (newConnections.offer(ch)) {
                accepted = true;
            } else if (mayBlock) {
                long startNs = time.nanoseconds();
                try {
                    newConnections.put(ch);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                acceptorBlockedPercentMeter.mark(time.nanoseconds() - startNs);
                accepted = true;
            } else {
                accepted = false;
            }
            if (accepted) wakeup();
            return accepted;
        }

        private void configureNewConnections() {
            int connectionsProcessed = 0;
            while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty()) {
                SocketChannel ch = newConnections.poll();
                if (ch == null) break;
                try {
                    LOGGER.debug("Processor " + id + " listening to new connection from " + ch.socket().getRemoteSocketAddress());
                    selector.register(connectionId(ch.socket()), ch);
                    connectionsProcessed += 1;
                } catch (Throwable e) {
                    SocketAddress remote = ch.socket().getRemoteSocketAddress();
                    connectionQuotas.closeChannel(this, listenerName, ch);
                    processException("Processor " + id + " closed connection from " + remote, e);
                }
            }
        }

        private void closeAll() {
            while (!newConnections.isEmpty()) {
                SocketChannel ch = newConnections.poll();
                if (ch != null) CoreUtils.swallow(() -> { try { ch.close(); } catch (IOException ignored) {} }, this, Level.ERROR);
            }
            for (KafkaChannel ch : selector.channels()) {
                close(ch.id());
            }
            selector.close();
            metricsGroup.removeMetric(IdlePercentMetricName, Map.of(NetworkProcessorMetricTag, Integer.toString(id)));
        }

        protected String connectionId(Socket socket) {
            String connId = ServerConnectionId.generateConnectionId(socket, id, nextConnectionIndex);
            nextConnectionIndex = (nextConnectionIndex == Integer.MAX_VALUE) ? 0 : nextConnectionIndex + 1;
            return connId;
        }

        void enqueueResponse(RequestChannel.Response response) {
            responseQueue.add(response);
            wakeup();
        }

        private RequestChannel.Response dequeueResponse() {
            RequestChannel.Response r = responseQueue.poll();
            if (r != null) r.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds();
            return r;
        }

        int responseQueueSize() { return responseQueue.size(); }

        int inflightResponseCount() { return inflightResponses.size(); }

        Optional<KafkaChannel> openOrClosingChannel(String connectionId) {
            KafkaChannel ch = selector.channel(connectionId);
            if (ch != null) return Optional.of(ch);
            KafkaChannel cc = selector.closingChannel(connectionId);
            return Optional.ofNullable(cc);
        }

        private void handleChannelMuteEvent(String connectionId, ChannelMuteEvent event) {
            openOrClosingChannel(connectionId).ifPresent(c -> c.handleChannelMuteEvent(event));
        }

        private void tryUnmuteChannel(String connectionId) {
            openOrClosingChannel(connectionId).ifPresent(c -> selector.unmute(c.id()));
        }

        Optional<KafkaChannel> channel(String connectionId) {
            return Optional.ofNullable(selector.channel(connectionId));
        }

        public void start() {
            if (!started.getAndSet(true)) {
                thread.start();
            }
        }

        public void wakeup() { selector.wakeup(); }

        public void beginShutdown() {
            if (shouldRun.getAndSet(false)) wakeup();
        }

        public void close() {
            try {
                beginShutdown();
                thread.join();
                if (!started.get()) CoreUtils.swallow(this::closeAll, this, Level.ERROR);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                metricsGroup.removeMetric("IdlePercent", Map.of("networkProcessor", Integer.toString(id)));
                metrics.removeMetric(expiredConnectionsKilledCountMetricName);
            }
        }
    }
}