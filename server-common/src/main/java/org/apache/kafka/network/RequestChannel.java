/*
 * Licensed to the Apache Software Foundation (ASF) ...
 */

package org.apache.kafka.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.yammer.metrics.core.Meter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.network.metrics.RequestChannelMetrics;
import org.apache.kafka.network.metrics.RequestMetrics;
import org.apache.kafka.server.KafkaConfig;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class RequestChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestChannel.class);

    // ===== Companion (object) port =====
    public static final class Companion {
        private static final Logger requestLogger = LoggerFactory.getLogger("kafka.request.logger");

        static final String RequestQueueSizeMetric = "RequestQueueSize";
        static final String ResponseQueueSizeMetric = "ResponseQueueSize";
        public static final String ProcessorMetricTag = "processor";

        /** Deprecated protocol APIs log at INFO; others at DEBUG. */
        static boolean isRequestLoggingEnabled(RequestHeader header) {
            return requestLogger.isDebugEnabled() || (requestLogger.isInfoEnabled() && header.isApiVersionDeprecated());
        }

        // Marker types from Scala
        public sealed interface BaseRequest permits ShutdownRequest, WakeupRequest, CallbackRequest, Request { }
        public static final class ShutdownRequest implements BaseRequest { private ShutdownRequest() {} public static final ShutdownRequest INSTANCE = new ShutdownRequest(); }
        public static final class WakeupRequest implements BaseRequest { private WakeupRequest() {} public static final WakeupRequest INSTANCE = new WakeupRequest(); }

        public static final class CallbackRequest implements BaseRequest {
            public final java.util.function.Consumer<RequestLocal> fun;
            public final Request originalRequest;
            public CallbackRequest(java.util.function.Consumer<RequestLocal> fun, Request originalRequest) {
                this.fun = fun; this.originalRequest = originalRequest;
            }
        }

        private static boolean shouldReturnNotController(AbstractResponse response) {
            if (response instanceof DescribeQuorumResponse dq) {
                return dq.errorCounts().containsKey(Errors.NOT_LEADER_OR_FOLLOWER);
            }
            return response.errorCounts().containsKey(Errors.NOT_CONTROLLER);
        }
    }

    // ====== Response hierarchy ======
    public static abstract class Response {
        public final Request request;
        protected Response(Request request) { this.request = request; }
        public int processor() { return request.processor; }
        public Optional<JsonNode> responseLog() { return Optional.empty(); }
        public Optional<Consumer<Send>> onComplete() { return Optional.empty(); }
    }

    public static final class SendResponse extends Response {
        public final Send responseSend;
        private final Optional<JsonNode> responseLogValue;
        private final Optional<Consumer<Send>> onCompleteCallback;
        public SendResponse(Request request, Send responseSend, Optional<JsonNode> responseLogValue, Optional<Consumer<Send>> onComplete) {
            super(request);
            this.responseSend = responseSend;
            this.responseLogValue = responseLogValue;
            this.onCompleteCallback = onComplete;
        }
        @Override public Optional<JsonNode> responseLog() { return responseLogValue; }
        @Override public Optional<Consumer<Send>> onComplete() { return onCompleteCallback; }
        @Override public String toString() { return "Response(type=Send, request="+request+", send="+responseSend+", asString="+responseLogValue+")"; }
    }
    public static final class NoOpResponse extends Response { public NoOpResponse(Request r){ super(r);} @Override public String toString(){return "Response(type=NoOp, request="+request+")";} }
    public static final class CloseConnectionResponse extends Response { public CloseConnectionResponse(Request r){ super(r);} @Override public String toString(){return "Response(type=CloseConnection, request="+request+")";} }
    public static final class StartThrottlingResponse extends Response { public StartThrottlingResponse(Request r){ super(r);} @Override public String toString(){return "Response(type=StartThrottling, request="+request+")";} }
    public static final class EndThrottlingResponse extends Response { public EndThrottlingResponse(Request r){ super(r);} @Override public String toString(){return "Response(type=EndThrottling, request="+request+")";} }

    // ====== Request ======
    public static final class Request implements Companion.BaseRequest {
        private static final Logger LOGGER = LoggerFactory.getLogger(Request.class);

        public final int processor;
        public final RequestContext context;
        public final long startTimeNanos;
        public final MemoryPool memoryPool;
        public volatile ByteBuffer buffer;
        public final RequestChannelMetrics metrics;
        public final Optional<RequestChannel.Request> envelope;

        // Volatile timings (written by handlers or purgatory)
        public volatile long requestDequeueTimeNanos = -1L;
        public volatile long apiLocalCompleteTimeNanos = -1L;
        public volatile long responseCompleteTimeNanos = -1L;
        public volatile long responseDequeueTimeNanos = -1L;
        public volatile long messageConversionsTimeNanos = 0L;
        public volatile long apiThrottleTimeMs = 0L;
        public volatile long temporaryMemoryBytes = 0L;
        public volatile Optional<Consumer<Long>> recordNetworkThreadTimeCallback = Optional.empty();
        public volatile Optional<Long> callbackRequestDequeueTimeNanos = Optional.empty();
        public volatile Optional<Long> callbackRequestCompleteTimeNanos = Optional.empty();

        public final Session session;

        private final RequestAndSize bodyAndSize;
        public final Optional<JsonNode> requestLog;

        public Request(int processor,
                       RequestContext context,
                       long startTimeNanos,
                       MemoryPool memoryPool,
                       ByteBuffer buffer,
                       RequestChannelMetrics metrics,
                       Optional<RequestChannel.Request> envelope) {
            this.processor = processor;
            this.context = context;
            this.startTimeNanos = startTimeNanos;
            this.memoryPool = memoryPool;
            this.buffer = buffer;
            this.metrics = metrics;
            this.envelope = envelope;

            this.session = new Session(context.principal(), context.clientAddress());
            this.bodyAndSize = context.parseRequest(buffer);

            // precompute JSON for request logging (before purgatory might drop payloads)
            this.requestLog = Companion.isRequestLoggingEnabled(context.header)
                ? Optional.of(RequestConvertToJson.request(loggableRequest()))
                : Optional.empty();

            // early release if possible
            if (!context.header.apiKey().requiresDelayedAllocation) {
                releaseBuffer();
            }

            LOGGER.trace("Processor " + processor + " received request: " + requestDesc(true));
        }

        public RequestHeader header() { return context.header; }
        private int sizeOfBodyInBytes() { return bodyAndSize.size; }
        public int sizeInBytes() { return header().size() + sizeOfBodyInBytes(); }

        public boolean isForwarded() { return envelope.isPresent(); }

        public <T extends AbstractRequest> T body(Class<T> cls) {
            AbstractRequest r = bodyAndSize.request;
            if (!cls.isInstance(r))
                throw new ClassCastException("Expected " + cls + " but found " + r.getClass());
            return cls.cast(r);
        }

        private AbstractRequest sanitizeConfigsIfNeeded(AbstractRequest req) {
            if (req instanceof AlterConfigsRequest ac) {
                var newData = ac.data().duplicate();
                newData.resources().forEach(resource -> {
                    var type = ConfigResource.Type.forId(resource.resourceType());
                    resource.configs().forEach(cfg ->
                        cfg.setValue(KafkaConfig.loggableValue(type, cfg.name(), cfg.value())));
                });
                return new AlterConfigsRequest(newData, ac.version());
            } else if (req instanceof IncrementalAlterConfigsRequest iac) {
                var newData = iac.data().duplicate();
                newData.resources().forEach(resource -> {
                    var type = ConfigResource.Type.forId(resource.resourceType());
                    resource.configs().forEach(cfg ->
                        cfg.setValue(KafkaConfig.loggableValue(type, cfg.name(), cfg.value())));
                });
                return new IncrementalAlterConfigsRequest.Builder(newData).build(iac.version());
            }
            return req;
        }

        public AbstractRequest loggableRequest() {
            return sanitizeConfigsIfNeeded(bodyAndSize.request);
        }

        private boolean shouldReturnNotController(AbstractResponse response) {
            return Companion.shouldReturnNotController(response);
        }

        public Send buildResponseSend(AbstractResponse abstractResponse) {
            if (envelope.isPresent()) {
                AbstractResponse envelopeResponse;
                if (shouldReturnNotController(abstractResponse)) {
                    envelopeResponse = new EnvelopeResponse(new EnvelopeResponseData()
                        .setErrorCode(Errors.NOT_CONTROLLER.code()));
                } else {
                    ByteBuffer responseBytes = context.buildResponseEnvelopePayload(abstractResponse);
                    envelopeResponse = new EnvelopeResponse(responseBytes, Errors.NONE);
                }
                return envelope.get().context.buildResponseSend(envelopeResponse);
            } else {
                return context.buildResponseSend(abstractResponse);
            }
        }

        public Optional<JsonNode> responseNode(AbstractResponse response) {
            return Companion.isRequestLoggingEnabled(context.header)
                ? Optional.of(RequestConvertToJson.response(response, context.apiVersion()))
                : Optional.empty();
        }

        public RequestHeader headerForLoggingOrThrottling() {
            return envelope.map(r -> r.context.header).orElseGet(this::header);
        }

        public String requestDesc(boolean details) {
            String fwd = envelope.map(r -> "Forwarded request: " + r.context + " ").orElse("");
            return fwd + header() + " -- " + loggableRequest().toString(details);
        }

        public long requestThreadTimeNanos() {
            if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds();
            return Math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L);
        }

        public void updateRequestMetrics(long networkThreadTimeNanos, Response response) {
            long endTimeNanos = Time.SYSTEM.nanoseconds();

            // nanos → millis with micros precision
            java.util.function.Function<Long, Double> nanosToMs = nanos -> {
                long pos = Math.max(nanos, 0L);
                return NANOSECONDS.toMicros(pos) / (double) MILLISECONDS.toMicros(1);
            };

            double requestQueueTimeMs = nanosToMs.apply(requestDequeueTimeNanos - startTimeNanos);
            long callbackDelta = callbackRequestCompleteTimeNanos.orElse(0L) - callbackRequestDequeueTimeNanos.orElse(0L);
            double apiLocalTimeMs = nanosToMs.apply(apiLocalCompleteTimeNanos - requestDequeueTimeNanos + callbackDelta);
            double apiRemoteTimeMs = nanosToMs.apply(responseCompleteTimeNanos - apiLocalCompleteTimeNanos - callbackDelta);
            double responseQueueTimeMs = nanosToMs.apply(responseDequeueTimeNanos - responseCompleteTimeNanos);
            double responseSendTimeMs = nanosToMs.apply(endTimeNanos - responseDequeueTimeNanos);
            double messageConversionsTimeMs = nanosToMs.apply(messageConversionsTimeNanos);
            double totalTimeMs = nanosToMs.apply(endTimeNanos - startTimeNanos);

            List<String> overrideMetricNames = new ArrayList<>();
            if (header().apiKey() == ApiKeys.FETCH) {
                String specified = body(FetchRequest.class).isFromFollower()
                    ? RequestMetrics.FOLLOW_FETCH_METRIC_NAME
                    : RequestMetrics.CONSUMER_FETCH_METRIC_NAME;
                overrideMetricNames.add(specified);
                overrideMetricNames.add(header().apiKey().name);
            } else if (header().apiKey() == ApiKeys.ADD_PARTITIONS_TO_TXN &&
                body(AddPartitionsToTxnRequest.class).allVerifyOnlyRequest()) {
                overrideMetricNames.add(RequestMetrics.VERIFY_PARTITIONS_IN_TXN_METRIC_NAME);
            } else if (header().apiKey() == ApiKeys.LIST_CONFIG_RESOURCES && header().apiVersion() == 0) {
                overrideMetricNames.add(RequestMetrics.LIST_CLIENT_METRICS_RESOURCES_METRIC_NAME);
                overrideMetricNames.add(header().apiKey().name);
            } else {
                overrideMetricNames.add(header().apiKey().name);
            }

            for (String metricName : overrideMetricNames) {
                RequestMetrics m = metrics.apply(metricName);
                m.requestRate(header().apiVersion()).mark();
                m.deprecatedRequestRate(header().apiKey(), header().apiVersion(), context.clientInformation).ifPresent(Meter::mark);
                m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs));
                m.localTimeHist.update(Math.round(apiLocalTimeMs));
                m.remoteTimeHist.update(Math.round(apiRemoteTimeMs));
                m.throttleTimeHist.update(apiThrottleTimeMs);
                m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs));
                m.responseSendTimeHist.update(Math.round(responseSendTimeMs));
                m.totalTimeHist.update(Math.round(totalTimeMs));
                m.requestBytesHist.update(sizeOfBodyInBytes());
                m.messageConversionsTimeHist.ifPresent(h -> h.update(Math.round(messageConversionsTimeMs)));
                m.tempMemoryBytesHist.ifPresent(h -> h.update(temporaryMemoryBytes));
            }

            // Count time on network thread for quota
            recordNetworkThreadTimeCallback.ifPresent(cb -> cb.accept(networkThreadTimeNanos));

            if (Companion.isRequestLoggingEnabled(header())) {
                String desc = String.valueOf(RequestConvertToJson.requestDescMetrics(
                    header(), requestLog, response.responseLog(), context, session, isForwarded(),
                    totalTimeMs, requestQueueTimeMs, apiLocalTimeMs, apiRemoteTimeMs, apiThrottleTimeMs,
                    responseQueueTimeMs, responseSendTimeMs, temporaryMemoryBytes, messageConversionsTimeMs));

                String prefix = "Completed request:{}";
                if (header().isApiVersionDeprecated())
                    Companion.requestLogger.info(prefix, desc);
                else
                    Companion.requestLogger.debug(prefix, desc);
            }
        }

        public void releaseBuffer() {
            if (envelope.isPresent()) {
                envelope.get().releaseBuffer();
            } else if (buffer != null) {
                memoryPool.release(buffer);
                buffer = null;
            }
        }

        public void setRecordNetworkThreadTimeCallback(Consumer<Long> callback) {
            this.recordNetworkThreadTimeCallback = Optional.ofNullable(callback);
        }

        @Override public String toString() {
            return "Request(processor=" + processor +
                ", connectionId=" + context.connectionId() +
                ", session=" + session +
                ", listenerName=" + context.listenerName() +
                ", securityProtocol=" + context.securityProtocol() +
                ", buffer=" + buffer +
                ", envelope=" + envelope + ")";
        }
    }

    // ====== RequestChannel (instance) ======
    public final int queueSize;
    private final Time time;
    public final RequestChannelMetrics metrics;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());
    private final ArrayBlockingQueue<Companion.BaseRequest> requestQueue;
    private final ConcurrentHashMap<Integer, SocketServer.Processor> processors = new ConcurrentHashMap<>();
    private final ArrayBlockingQueue<Companion.BaseRequest> callbackQueue;

    public RequestChannel(int queueSize, Time time, RequestChannelMetrics metrics) {
        this.queueSize = queueSize;
        this.time = time;
        this.metrics = metrics;

        this.requestQueue = new ArrayBlockingQueue<>(queueSize);
        this.callbackQueue = new ArrayBlockingQueue<>(queueSize);

        metricsGroup.newGauge(Companion.RequestQueueSizeMetric, requestQueue::size);

        metricsGroup.newGauge(Companion.ResponseQueueSizeMetric, () -> {
            int total = 0;
            for (SocketServer.Processor p : processors.values()) total += p.responseQueueSize();
            return total;
        });
    }

    public void addProcessor(SocketServer.Processor processor) {
        if (processors.putIfAbsent(processor.id, processor) != null) {
            LOGGER.warn("Unexpected processor with processorId " + processor.id);
        }
        metricsGroup.newGauge(
            Companion.ResponseQueueSizeMetric,
            processor::responseQueueSize,
            Map.of(Companion.ProcessorMetricTag, Integer.toString(processor.id))
        );
    }

    public void removeProcessor(int processorId) {
        processors.remove(processorId);
        metricsGroup.removeMetric(Companion.ResponseQueueSizeMetric, Map.of(Companion.ProcessorMetricTag, Integer.toString(processorId)));
    }

    /** Enqueue a request for handling (blocks if queue is full). */
    public void sendRequest(Request request) {
        requestQueue.add(request);
    }

    /** Close connection (no response expected, e.g., produce acks=0). */
    public void closeConnection(Request request, Map<Errors, Integer> errorCounts) {
        updateErrorMetrics(request.header().apiKey(), errorCounts);
        sendResponse(new CloseConnectionResponse(request));
    }

    public void sendResponse(Request request, AbstractResponse response, Optional<Consumer<Send>> onComplete) {
        updateErrorMetrics(request.header().apiKey(), response.errorCounts());
        sendResponse(new SendResponse(
            request,
            request.buildResponseSend(response),
            request.responseNode(response),
            onComplete
        ));
    }

    public void sendNoOpResponse(Request request) {
        sendResponse(new NoOpResponse(request));
    }

    public void startThrottling(Request request) {
        sendResponse(new StartThrottlingResponse(request));
    }

    public void endThrottling(Request request) {
        sendResponse(new EndThrottlingResponse(request));
    }

    /** Back to processor to put on the wire. */
    void sendResponse(Response response) {
        if (LOGGER.isTraceEnabled()) {
            RequestHeader hdr = response.request.headerForLoggingOrThrottling();
            String msg;
            if (response instanceof SendResponse sr) {
                msg = "Sending " + hdr.apiKey() + " response to client " + hdr.clientId() + " of " + sr.responseSend.size() + " bytes.";
            } else if (response instanceof NoOpResponse) {
                msg = "Not sending " + hdr.apiKey() + " response to client " + hdr.clientId() + " as it's not required.";
            } else if (response instanceof CloseConnectionResponse) {
                msg = "Closing connection for client " + hdr.clientId() + " due to error during " + hdr.apiKey() + ".";
            } else if (response instanceof StartThrottlingResponse) {
                msg = "Notifying channel throttling has started for client " + hdr.clientId() + " for " + hdr.apiKey();
            } else if (response instanceof EndThrottlingResponse) {
                msg = "Notifying channel throttling has ended for client " + hdr.clientId() + " for " + hdr.apiKey();
            } else {
                msg = "Unknown response type for " + hdr.apiKey();
            }
            LOGGER.trace(msg);
        }

        // Update timing marks when a terminal response is produced
        if (response instanceof SendResponse || response instanceof NoOpResponse || response instanceof CloseConnectionResponse) {
            Request req = response.request;
            long now = time.nanoseconds();
            req.responseCompleteTimeNanos = now;
            if (req.apiLocalCompleteTimeNanos == -1L) req.apiLocalCompleteTimeNanos = now;
            if (req.callbackRequestDequeueTimeNanos.isPresent() && req.callbackRequestCompleteTimeNanos.isEmpty()) {
                req.callbackRequestCompleteTimeNanos = Optional.of(time.nanoseconds());
            }
        }

        SocketServer.Processor p = processors.get(response.processor());
        if (p != null) {
            p.enqueueResponse(response);
        }
        // else processor already shut down; drop response
    }

    /** Poll with timeout, preferring callback queue work. */
    public Companion.BaseRequest receiveRequest(long timeoutMs) throws InterruptedException {
        Companion.BaseRequest cb = callbackQueue.poll();
        if (cb != null) return cb;

        Companion.BaseRequest req = requestQueue.poll(timeoutMs, MILLISECONDS);
        if (req == Companion.WakeupRequest.INSTANCE) {
            Companion.BaseRequest nextCb = callbackQueue.poll();
            return (nextCb != null) ? nextCb : req;
        }
        return req;
    }

    /** Take (blocking). */
    public Companion.BaseRequest receiveRequest() throws InterruptedException {
        return requestQueue.take();
    }

    public void updateErrorMetrics(ApiKeys apiKey, Map<Errors, Integer> errors) {
        RequestMetrics m = metrics.apply(apiKey.name);
        errors.forEach((error, count) -> m.markErrorMeter(error, count));
    }

    public void clear() {
        requestQueue.clear();
        callbackQueue.clear();
    }

    public void shutdown() {
        clear();
        metrics.close();
    }

    public void sendShutdownRequest() {
        requestQueue.add(Companion.ShutdownRequest.INSTANCE);
    }

    public void sendCallbackRequest(Companion.CallbackRequest request) {
        callbackQueue.add(request);
        if (!requestQueue.offer(Companion.WakeupRequest.INSTANCE)) {
            LOGGER.trace("Wakeup request could not be added to queue. This means queue is full, so we will still process callback.");
        }
    }
}