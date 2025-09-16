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

package org.apache.kafka.server;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.Group.GroupType;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.security.authorizer.AuthorizerUtils;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.utils.CoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Java port of the Scala KafkaConfig shown in the snippet.
 * Notes:
 * - Companion-object helpers are represented as static methods on this class.
 * - Methods/fields that didn't affect external API flow are kept but trimmed where possible.
 * - Dynamic config delegation semantics (currentConfig swapping) are preserved.
 */
public class KafkaConfig extends AbstractKafkaConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConfig.class);

    /** ========= static (companion object) ========= */

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toHtml(4, cfg -> "brokerconfigs_" + cfg,
            DynamicBrokerConfig.dynamicConfigUpdateModes()));
    }

    public static final ConfigDef CONFIG_DEF = AbstractKafkaConfig.CONFIG_DEF;

    public static List<String> configNames() {
        return CONFIG_DEF.names().stream().sorted().collect(Collectors.toList());
    }

    static Map<String, Object> defaultValues() {
        return CONFIG_DEF.defaultValues();
    }

    static Map<String, ConfigDef.ConfigKey> configKeys() {
        return CONFIG_DEF.configKeys();
    }

    public static KafkaConfig fromProps(Properties props) {
        return fromProps(props, true);
    }

    public static KafkaConfig fromProps(Properties props, boolean doLog) {
        return new KafkaConfig(props, doLog);
    }

    public static KafkaConfig fromProps(Properties defaults, Properties overrides) {
        return fromProps(defaults, overrides, true);
    }

    public static KafkaConfig fromProps(Properties defaults, Properties overrides, boolean doLog) {
        Properties p = new Properties();
        p.putAll(defaults);
        p.putAll(overrides);
        return fromProps(p, doLog);
    }

    public static KafkaConfig apply(Map<?, ?> props, boolean doLog) {
        return new KafkaConfig(props, doLog);
    }

    private static Optional<ConfigDef.Type> typeOf(String name) {
        ConfigDef.ConfigKey k = CONFIG_DEF.configKeys().get(name);
        return Optional.ofNullable(k).map(ck -> ck.type);
    }

    public static Optional<ConfigDef.Type> configType(String configName) {
        Optional<ConfigDef.Type> exact = configTypeExact(configName);
        if (exact.isPresent()) return exact;
        Optional<ConfigDef.Type> t = typeOf(configName);
        if (t.isPresent()) return t;
        // search dynamic synonyms
        return DynamicBrokerConfig.brokerConfigSynonyms(configName, true)
            .stream().map(KafkaConfig::typeOf).filter(Optional::isPresent).map(Optional::get).findFirst();
    }

    private static Optional<ConfigDef.Type> configTypeExact(String exactName) {
        ConfigDef.Type t = typeOf(exactName).orElse(null);
        if (t != null) return Optional.of(t);
        ConfigDef.ConfigKey k = DynamicConfig.Broker.configKeys().get(exactName);
        return (k != null) ? Optional.of(k.type) : Optional.empty();
    }

    public static boolean maybeSensitive(Optional<ConfigDef.Type> t) {
        return t.isEmpty() || t.get() == ConfigDef.Type.PASSWORD;
    }

    public static String loggableValue(ConfigResource.Type resourceType, String name, String value) {
        boolean sensitive;
        switch (resourceType) {
            case BROKER -> sensitive = maybeSensitive(configType(name));
            case TOPIC -> sensitive = maybeSensitive(LogConfig.configType(name).toJavaUtil());
            case GROUP -> sensitive = maybeSensitive(GroupConfig.configType(name).toJavaUtil());
            case BROKER_LOGGER, CLIENT_METRICS -> sensitive = false;
            default -> sensitive = true;
        }
        return sensitive ? Password.HIDDEN : value;
    }

    /** Copy a configuration map, populating keys we treat as synonyms. */
    public static Map<Object, Object> populateSynonyms(Map<?, ?> input) {
        Map<Object, Object> out = new HashMap<>(input.size());
        out.putAll(input);
        Object brokerId = out.get(ServerConfigs.BROKER_ID_CONFIG);
        Object nodeId = out.get(KRaftConfigs.NODE_ID_CONFIG);
        if (brokerId == null && nodeId != null) {
            out.put(ServerConfigs.BROKER_ID_CONFIG, nodeId);
        } else if (brokerId != null && nodeId == null) {
            out.put(KRaftConfigs.NODE_ID_CONFIG, brokerId);
        }
        return out;
    }

    /** ========= instance ========= */

    private volatile KafkaConfig currentConfig = this;
    public final Set<ProcessRole> processRoles;
    final DynamicBrokerConfig dynamicConfig = new DynamicBrokerConfig(this);

    private final RemoteLogManagerConfig _remoteLogManagerConfig = new RemoteLogManagerConfig(this);
    public RemoteLogManagerConfig remoteLogManagerConfig() { return _remoteLogManagerConfig; }

    private final QuorumConfig _quorumConfig = new QuorumConfig(this);
    public QuorumConfig quorumConfig() { return _quorumConfig; }

    private final GroupCoordinatorConfig _groupCoordinatorConfig = new GroupCoordinatorConfig(this);
    public GroupCoordinatorConfig groupCoordinatorConfig() { return _groupCoordinatorConfig; }

    private final ShareGroupConfig _shareGroupConfig = new ShareGroupConfig(this);
    public ShareGroupConfig shareGroupConfig() { return _shareGroupConfig; }

    private final ShareCoordinatorConfig _shareCoordinatorConfig = new ShareCoordinatorConfig(this);
    public ShareCoordinatorConfig shareCoordinatorConfig() { return _shareCoordinatorConfig; }

    private final QuotaConfig _quotaConfig = new QuotaConfig(this);
    public QuotaConfig quotaConfig() { return _quotaConfig; }

    // ===== General Configs / Accessors =====

    public final int nodeId = getInt(KRaftConfigs.NODE_ID_CONFIG);
    public final int initialRegistrationTimeoutMs = getInt(KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG);
    public final int brokerHeartbeatIntervalMs = getInt(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG);
    public final int brokerSessionTimeoutMs = getInt(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG);
    public final long controllerPerformanceSamplePeriodMs = getLong(KRaftConfigs.CONTROLLER_PERFORMANCE_SAMPLE_PERIOD_MS);
    public final long controllerPerformanceAlwaysLogThresholdMs = getLong(KRaftConfigs.CONTROLLER_PERFORMANCE_ALWAYS_LOG_THRESHOLD_MS);

    public KafkaConfig(Map<?, ?> props) {
        this(true, populateSynonyms(props));
    }

    public KafkaConfig(Map<?, ?> props, boolean doLog) {
        this(doLog, populateSynonyms(props));
    }

    private KafkaConfig(boolean doLog, Map<?, ?> props) {
        super(CONFIG_DEF, props, Utils.castToStringObjectMap(props), doLog);
        this.processRoles = parseProcessRoles();
        validateValues(); // do the validations at the end of construction
    }

    private Set<ProcessRole> parseProcessRoles() {
        List<String> roles = getList(KRaftConfigs.PROCESS_ROLES_CONFIG);
        return roles.stream().map(String::toLowerCase).map(r -> switch (r) {
            case "broker" -> ProcessRole.BrokerRole;
            case "controller" -> ProcessRole.ControllerRole;
            default -> throw new ConfigException("Unknown process role '" + r + "' (only 'broker' and 'controller' are allowed roles)");
        }).collect(Collectors.toSet());
    }

    public boolean isKRaftCombinedMode() {
        return processRoles.equals(Set.of(ProcessRole.BrokerRole, ProcessRole.ControllerRole));
    }

    public String metadataLogDir() {
        String dir = getString(MetadataLogConfig.METADATA_LOG_DIR_CONFIG);
        return (dir != null) ? dir : logDirs().get(0);
    }

    public long serverMaxStartupTimeMs() {
        return getLong(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG);
    }

    public int messageMaxBytes() { return getInt(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG); }
    public long connectionSetupTimeoutMs() { return getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG); }
    public long connectionSetupTimeoutMaxMs() { return getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG); }

    /** ===== Expose the handful of getters widely used around the server/network layers ===== */

    public int maxConnectionsPerIp() { return getInt(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG); }

    public Map<String, Integer> maxConnectionsPerIpOverrides() {
        String raw = getString(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG);
        Map<String, Object> m = AbstractKafkaConfig.getMap(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, raw);
        Map<String, Integer> out = new HashMap<>();
        for (Map.Entry<String, Object> e : m.entrySet()) out.put(e.getKey(), Integer.parseInt(String.valueOf(e.getValue())));
        return out;
    }

    public int maxConnections() { return getInt(SocketServerConfigs.MAX_CONNECTIONS_CONFIG); }

    public int maxConnectionCreationRate() { return getInt(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG); }

    public List<Endpoint> listeners() {
        return CoreUtils.listenerListToEndPoints(getList(SocketServerConfigs.LISTENERS_CONFIG), effectiveListenerSecurityProtocolMap());
    }

    public List<Endpoint> controllerListeners() {
        Set<ListenerName> controllerNames = controllerListenerNames();
        return listeners().stream().filter(e -> controllerNames.contains(e.listener())).collect(Collectors.toList());
    }

    public String saslMechanismInterBrokerProtocol() {
        return getString(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG);
    }

    /** ===== Dynamic Config surface ===== */

    public void addReconfigurable(Reconfigurable r) {
        dynamicConfig.addReconfigurable(r);
    }

    public void removeReconfigurable(Reconfigurable r) {
        dynamicConfig.removeReconfigurable(r);
    }

    /** ===== Current-config delegation (matches Scala volatile indirection) ===== */

    void updateCurrentConfig(KafkaConfig newConfig) {
        this.currentConfig = newConfig;
    }

    @Override
    public Map<String, Object> originals() {
        return (this == currentConfig) ? super.originals() : currentConfig.originals();
    }

    @Override
    public Map<String, ?> values() {
        return (this == currentConfig) ? super.values() : currentConfig.values();
    }

    @Override
    public Map<String, ?> nonInternalValues() {
        return (this == currentConfig) ? super.nonInternalValues() : currentConfig.nonInternalValues();
    }

    @Override
    public Map<String, String> originalsStrings() {
        return (this == currentConfig) ? super.originalsStrings() : currentConfig.originalsStrings();
    }

    @Override
    public Map<String, Object> originalsWithPrefix(String prefix) {
        return (this == currentConfig) ? super.originalsWithPrefix(prefix) : currentConfig.originalsWithPrefix(prefix);
    }

    @Override
    public Map<String, Object> valuesWithPrefixOverride(String prefix) {
        return (this == currentConfig) ? super.valuesWithPrefixOverride(prefix) : currentConfig.valuesWithPrefixOverride(prefix);
    }

    @Override
    public Object get(String key) {
        return (this == currentConfig) ? super.get(key) : currentConfig.get(key);
    }

    /** The “this config only” accessors used by DynamicBrokerConfig */
    Map<String, Object> originalsFromThisConfig() { return super.originals(); }
    Map<String, ?> valuesFromThisConfig() { return super.values(); }
    Map<String, Object> valuesFromThisConfigWithPrefixOverride(String prefix) { return super.valuesWithPrefixOverride(prefix); }

    /** ===== A subset of the many fields / validations from the Scala version ===== */

    public Set<ListenerName> earlyStartListeners() {
        Set<ListenerName> listenersSet = listeners().stream().map(e -> ListenerName.normalised(e.listener())).collect(Collectors.toSet());
        Set<ListenerName> controllerSet = controllerListeners().stream().map(e -> ListenerName.normalised(e.listener())).collect(Collectors.toSet());

        List<String> raw = getList(ServerConfigs.EARLY_START_LISTENERS_CONFIG);
        if (raw == null) return controllerSet;

        Set<ListenerName> out = new HashSet<>();
        for (String s : raw) {
            String str = s.trim();
            if (str.isEmpty()) continue;
            ListenerName ln = new ListenerName(str);
            if (!listenersSet.contains(ln) && !controllerSet.contains(ln)) {
                throw new ConfigException(ServerConfigs.EARLY_START_LISTENERS_CONFIG + " contains listener " + ln.value() +
                    ", but this is not contained in " + SocketServerConfigs.LISTENERS_CONFIG + " or " + KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG);
            }
            out.add(ln);
        }
        return out;
    }

    /** Basic validations needed for net/raft/bootstrap usage (trimmed but equivalent for semantics used elsewhere). */
    private void validateValues() {
        if (nodeId != brokerId()) {
            throw new ConfigException("You must set `" + KRaftConfigs.NODE_ID_CONFIG + "` to the same value as `" + ServerConfigs.BROKER_ID_CONFIG + "`.");
        }

        // Example of time/retention guards from the Scala code (kept concise):
        if (logDirs().isEmpty()) throw new ConfigException("At least one log directory must be defined via log.dirs or log.dir.");

        // Heartbeat/session relation
        if (brokerHeartbeatIntervalMs * 2L > brokerSessionTimeoutMs) {
            LOGGER.error(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG + " ({} ms) must be <= half of " + KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG + " ({} ms).",
                brokerHeartbeatIntervalMs, brokerSessionTimeoutMs);
        }

        // KRaft role validations (kept faithful to original intent)
        Set<Integer> voterIds = QuorumConfig.parseVoterIds(quorumConfig().voters());
        if (processRoles.equals(Set.of(ProcessRole.BrokerRole))) {
            if (voterIds.isEmpty() && quorumConfig().bootstrapServers().isEmpty()) {
                throw new ConfigException("If using " + KRaftConfigs.PROCESS_ROLES_CONFIG + ", either " +
                    QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG + " or " + QuorumConfig.QUORUM_VOTERS_CONFIG + " must be set.");
            }
            if (voterIds.contains(nodeId)) {
                throw new ConfigException("Broker-only nodeId " + nodeId + " must not be included in " +
                    QuorumConfig.QUORUM_VOTERS_CONFIG + "=" + voterIds);
            }
            if (controllerListenerNames().isEmpty()) {
                throw new ConfigException(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG +
                    " must contain at least one value when running KRaft with just the broker role");
            }
            if (!controllerListeners().isEmpty()) {
                throw new ConfigException(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG +
                    " must not contain a value appearing in " + SocketServerConfigs.LISTENERS_CONFIG + " when broker-only");
            }
            // warn for misplaced policies
            if (originals().containsKey(ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG)) {
                LOGGER.warn(ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG + " should be defined in the controller role.");
            }
            if (originals().containsKey(ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG)) {
                LOGGER.warn(ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG + " should be defined in the controller role.");
            }
        } else if (processRoles.equals(Set.of(ProcessRole.ControllerRole))) {
            if (voterIds.isEmpty() && quorumConfig().bootstrapServers().isEmpty()) {
                throw new ConfigException("If using " + KRaftConfigs.PROCESS_ROLES_CONFIG + ", either " +
                    QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG + " or " + QuorumConfig.QUORUM_VOTERS_CONFIG + " must be set.");
            }
            // listeners must be only controller listeners
            if (effectiveAdvertisedControllerListeners().size() != listeners().size()) {
                throw new ConfigException(SocketServerConfigs.LISTENERS_CONFIG +
                    " must only contain controller listeners when " + KRaftConfigs.PROCESS_ROLES_CONFIG + "=controller");
            }
            if (getString(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG) != null) {
                if (controllerListenerNames().contains(interBrokerListenerName().value())) {
                    throw new ConfigException(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG +
                        " must not contain explicitly set " + ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG + " when controller-only");
                }
            }
            if (!voterIds.isEmpty() && !voterIds.contains(nodeId)) {
                throw new ConfigException("Controller nodeId " + nodeId + " must be in " + QuorumConfig.QUORUM_VOTERS_CONFIG + "=" + voterIds);
            }
            if (effectiveAdvertisedControllerListeners().isEmpty()) {
                throw new ConfigException(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG +
                    " must contain at least one value appearing in " + SocketServerConfigs.LISTENERS_CONFIG + " when controller");
            }
        } else if (isKRaftCombinedMode()) {
            if (voterIds.isEmpty() && quorumConfig().bootstrapServers().isEmpty()) {
                throw new ConfigException("If using " + KRaftConfigs.PROCESS_ROLES_CONFIG + ", either " +
                    QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG + " or " + QuorumConfig.QUORUM_VOTERS_CONFIG + " must be set.");
            }
            if (!voterIds.isEmpty() && !voterIds.contains(nodeId)) {
                throw new ConfigException("Combined mode nodeId " + nodeId + " must be in " + QuorumConfig.QUORUM_VOTERS_CONFIG + "=" + voterIds);
            }
            if (effectiveAdvertisedControllerListeners().isEmpty()) {
                throw new ConfigException(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG +
                    " must contain at least one value appearing in " + SocketServerConfigs.LISTENERS_CONFIG + " when combined mode");
            }
        }

        // Inter-broker SASL check
        boolean interBrokerUsesSasl =
            interBrokerSecurityProtocol() == SecurityProtocol.SASL_PLAINTEXT ||
                interBrokerSecurityProtocol() == SecurityProtocol.SASL_SSL;
        if (interBrokerUsesSasl && !saslEnabledMechanisms(interBrokerListenerName()).contains(saslMechanismInterBrokerProtocol())) {
            throw new ConfigException(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG +
                " must be included in " + BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG + " when SASL is used for inter-broker communication");
        }

        // Principal builder presence
        Class<?> principalBuilderClass = getClass(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG);
        if (principalBuilderClass == null) {
            throw new ConfigException(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG + " must be non-null");
        }
        if (!KafkaPrincipalSerde.class.isAssignableFrom(principalBuilderClass)) {
            throw new ConfigException(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG + " must implement KafkaPrincipalSerde");
        }
    }

    /** ===== Helpers mirrored from Scala code (trimmed to common use) ===== */

    private Set<String> saslEnabledMechanisms(ListenerName listenerName) {
        Object v = valuesWithPrefixOverride(listenerName.configPrefix()).get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG);
        if (v instanceof List<?> l) {
            return l.stream().map(Object::toString).collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    public List<Endpoint> dataPlaneListeners() {
        Set<ListenerName> controllerNames = controllerListenerNames();
        return listeners().stream().filter(l -> !controllerNames.contains(l.listener())).collect(Collectors.toList());
    }

    public List<Endpoint> effectiveAdvertisedControllerListeners() {
        List<String> advertised = getList(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG);
        List<Endpoint> controllerAdvertised = (advertised != null)
            ? CoreUtils.listenerListToEndPoints(advertised, effectiveListenerSecurityProtocolMap(), false).stream()
            .filter(e -> controllerListenerNames().contains(e.listener()))
            .toList()
            : Collections.emptyList();

        List<Endpoint> controllerListenersValue = controllerListeners();

        List<Endpoint> out = new ArrayList<>();
        for (ListenerName name : controllerListenerNames()) {
            ListenerName norm = ListenerName.normalised(name.value());
            Optional<Endpoint> adv = controllerAdvertised.stream()
                .filter(e -> ListenerName.normalised(e.listener()).equals(norm))
                .findFirst();
            if (adv.isPresent()) {
                out.add(adv.get());
            } else {
                Optional<Endpoint> fromListeners = controllerListenersValue.stream()
                    .filter(e -> ListenerName.normalised(e.listener()).equals(norm))
                    .findFirst()
                    .map(e -> e.host().equals("0.0.0.0")
                        ? new Endpoint(e.listener(), e.securityProtocol(), null, e.port())
                        : e);
                fromListeners.ifPresent(out::add);
            }
        }
        return out;
    }

    public List<Endpoint> effectiveAdvertisedBrokerListeners() {
        List<String> advertised = getList(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG);
        List<Endpoint> base = (advertised != null)
            ? CoreUtils.listenerListToEndPoints(advertised, effectiveListenerSecurityProtocolMap(), false)
            : listeners();
        Set<ListenerName> controllerNames = controllerListenerNames();
        return base.stream().filter(e -> !controllerNames.contains(e.listener())).collect(Collectors.toList());
    }

    /** ===== Log-config extraction (used by log layer) ===== */
    public Map<String, Object> extractLogConfigMap() {
        Map<String, Object> m = new HashMap<>();
        m.put(TopicConfig.SEGMENT_BYTES_CONFIG, getInt(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG));
        m.put(TopicConfig.SEGMENT_MS_CONFIG, logRollTimeMillis());
        m.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, logRollTimeJitterMillis());
        m.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, getInt(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG));
        m.put(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG));
        m.put(TopicConfig.FLUSH_MS_CONFIG, logFlushIntervalMs());
        m.put(TopicConfig.RETENTION_BYTES_CONFIG, getLong(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG));
        m.put(TopicConfig.RETENTION_MS_CONFIG, logRetentionTimeMillis());
        m.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, messageMaxBytes());
        m.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, getInt(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG));
        m.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, getLong(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP));
        m.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, getLong(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP));
        m.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, getLong(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP));
        m.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, getLong(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG));
        m.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, getDouble(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP));
        m.put(TopicConfig.CLEANUP_POLICY_CONFIG, getList(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG));
        m.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, getInt(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG));
        m.put(TopicConfig.COMPRESSION_TYPE_CONFIG, getString(ServerConfigs.COMPRESSION_TYPE_CONFIG));
        m.put(TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG, getInt(ServerConfigs.COMPRESSION_GZIP_LEVEL_CONFIG));
        m.put(TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG, getInt(ServerConfigs.COMPRESSION_LZ4_LEVEL_CONFIG));
        m.put(TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, getInt(ServerConfigs.COMPRESSION_ZSTD_LEVEL_CONFIG));
        m.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, getBoolean(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG));
        m.put(TopicConfig.PREALLOCATE_CONFIG, getBoolean(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG));
        m.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.forName(getString(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG)).name);
        m.put(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG));
        m.put(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG));
        m.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, remoteLogManagerConfig().logLocalRetentionMs());
        m.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, remoteLogManagerConfig().logLocalRetentionBytes());
        return m;
    }

    /** ===== Retention helpers (preserve Scala semantics) ===== */

    public long logRollTimeMillis() {
        Long millis = getLong(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG);
        if (millis != null) return millis;
        return 60L * 60L * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG);
    }

    public long logRollTimeJitterMillis() {
        Long millis = getLong(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG);
        if (millis != null) return millis;
        return 60L * 60L * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG);
    }

    public long logFlushIntervalMs() {
        Long v = getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG);
        return (v != null) ? v : getLong(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG);
    }

    public long logRetentionTimeMillis() {
        long minute = 60_000L;
        long hour = 60L * minute;
        Long millis = getLong(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG);
        if (millis == null) {
            Integer mins = getInt(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG);
            millis = (mins != null) ? mins * minute : getInt(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG) * hour;
        }
        if (millis < 0) return -1L;
        return millis;
    }

    /** ===== Convenience: advertised listeners / inter-broker hooks ===== */

    public List<Endpoint> effectiveAdvertisedBrokerListenersChecked() {
        List<Endpoint> v = effectiveAdvertisedBrokerListeners();
        boolean anyZero = v.stream().anyMatch(e -> "0.0.0.0".equals(e.host()));
        if (anyZero) {
            throw new ConfigException(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG +
                " cannot use the nonroutable meta-address 0.0.0.0. Use a routable IP address.");
        }
        return v;
    }

    public List<Endpoint> effectiveAdvertisedControllerListenersChecked() {
        List<Endpoint> v = effectiveAdvertisedControllerListeners();
        boolean anyZero = v.stream().anyMatch(e -> "0.0.0.0".equals(e.host()));
        if (anyZero) {
            throw new ConfigException(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG +
                " cannot use the nonroutable meta-address 0.0.0.0. Use a routable IP address.");
        }
        return v;
    }

    /** ===== MetadataVersion checks (trim) ===== */

    public void validateWithMetadataVersion(MetadataVersion mv) {
        if (processRoles.contains(ProcessRole.BrokerRole) && logDirs().size() > 1) {
            if (!mv.isDirectoryAssignmentSupported()) {
                throw new ConfigException("Multiple log directories (JBOD) are not supported in current MetadataVersion " +
                    mv + ". Need " + MetadataVersion.IBP_3_7_IV2 + " or higher");
            }
        }
    }

    /** ===== Minimal shims for things referenced in the Scala code ===== */

    public Set<ListenerName> controllerListenerNames() {
        // in upstream it’s a List/Set of strings in config, normalized to ListenerName
        List<String> names = getList(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG);
        if (names == null) return Collections.emptySet();
        return names.stream().map(ListenerName::new).collect(Collectors.toSet());
    }

    public ListenerName interBrokerListenerName() {
        String v = getString(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG);
        return (v != null) ? new ListenerName(v) : ListenerName.forSecurityProtocol(interBrokerSecurityProtocol());
    }

    public SecurityProtocol interBrokerSecurityProtocol() {
        ListenerName l = interBrokerListenerName();
        return effectiveListenerSecurityProtocolMap().getOrDefault(ListenerName.normalised(l), SecurityProtocol.PLAINTEXT);
    }

    /** Create Authorizer instance (same behavior) */
    public Optional<Plugin<Authorizer>> createNewAuthorizer(Metrics metrics, String role) throws ClassNotFoundException {
        String className = getString(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG);
        if (className == null || className.isEmpty()) return Optional.empty();
        return Optional.of(AuthorizerUtils.createAuthorizer(className, originals(), metrics,
            ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, role));
    }

    /** Metric configs (common) */
    public int metricNumSamples() { return getInt(MetricConfigs.METRIC_NUM_SAMPLES_CONFIG); }
    public long metricSampleWindowMs() { return getLong(MetricConfigs.METRIC_SAMPLE_WINDOW_MS_CONFIG); }
    public String metricRecordingLevel() { return getString(MetricConfigs.METRIC_RECORDING_LEVEL_CONFIG); }

    /** Client Telemetry */
    public int clientTelemetryMaxBytes() { return getInt(MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_CONFIG); }

    /** Simple helpers to mirror Scala’s Option/collection APIs */
    private static long toMillis(long amount, TimeUnit unit) { return unit.toMillis(amount); }
}