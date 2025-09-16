/*
 * Licensed to the Apache Software Foundation (ASF)...
 * (same license header as your snippet)
 */

package org.apache.kafka.server.metadata;

import kafka.network.ConnectionQuotas;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.image.ClientQuotaDelta;
import org.apache.kafka.image.ClientQuotasDelta;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.quota.ClientQuotaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * A strict hierarchy of entities that we support
 */
sealed interface QuotaEntity permits
    QuotaEntity.IpEntity,
    QuotaEntity.DefaultIpEntity,
    QuotaEntity.UserEntity,
    QuotaEntity.DefaultUserEntity,
    QuotaEntity.ClientIdEntity,
    QuotaEntity.DefaultClientIdEntity,
    QuotaEntity.ExplicitUserExplicitClientIdEntity,
    QuotaEntity.ExplicitUserDefaultClientIdEntity,
    QuotaEntity.DefaultUserExplicitClientIdEntity,
    QuotaEntity.DefaultUserDefaultClientIdEntity {

    record IpEntity(String ip) implements QuotaEntity {
        public IpEntity {
            Objects.requireNonNull(ip, "ip");
        }
    }

    final class DefaultIpEntity implements QuotaEntity {
        private DefaultIpEntity() {
        }

        private static final DefaultIpEntity INSTANCE = new DefaultIpEntity();

        public static DefaultIpEntity get() {
            return INSTANCE;
        }
    }

    record UserEntity(String user) implements QuotaEntity {
        public UserEntity {
            Objects.requireNonNull(user, "user");
        }
    }

    final class DefaultUserEntity implements QuotaEntity {
        private DefaultUserEntity() {
        }

        private static final DefaultUserEntity INSTANCE = new DefaultUserEntity();

        public static DefaultUserEntity get() {
            return INSTANCE;
        }
    }

    record ClientIdEntity(String clientId) implements QuotaEntity {
        public ClientIdEntity {
            Objects.requireNonNull(clientId, "clientId");
        }
    }

    final class DefaultClientIdEntity implements QuotaEntity {
        private DefaultClientIdEntity() {
        }

        private static final DefaultClientIdEntity INSTANCE = new DefaultClientIdEntity();

        public static DefaultClientIdEntity get() {
            return INSTANCE;
        }
    }

    record ExplicitUserExplicitClientIdEntity(String user, String clientId) implements QuotaEntity {
        public ExplicitUserExplicitClientIdEntity {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(clientId, "clientId");
        }
    }

    record ExplicitUserDefaultClientIdEntity(String user) implements QuotaEntity {
        public ExplicitUserDefaultClientIdEntity {
            Objects.requireNonNull(user, "user");
        }
    }

    record DefaultUserExplicitClientIdEntity(String clientId) implements QuotaEntity {
        public DefaultUserExplicitClientIdEntity {
            Objects.requireNonNull(clientId, "clientId");
        }
    }

    final class DefaultUserDefaultClientIdEntity implements QuotaEntity {
        private DefaultUserDefaultClientIdEntity() {
        }

        private static final DefaultUserDefaultClientIdEntity INSTANCE = new DefaultUserDefaultClientIdEntity();

        public static DefaultUserDefaultClientIdEntity get() {
            return INSTANCE;
        }
    }
}

/**
 * Process quota metadata records as they appear in the metadata log and update quota managers and cache as necessary.
 */
public class ClientQuotaMetadataManager {
    private static final Logger log = LoggerFactory.getLogger(ClientQuotaMetadataManager.class);

    final QuotaManagers quotaManagers;
    final ConnectionQuotas connectionQuotas;

    public ClientQuotaMetadataManager(QuotaManagers quotaManagers, ConnectionQuotas connectionQuotas) {
        this.quotaManagers = Objects.requireNonNull(quotaManagers);
        this.connectionQuotas = Objects.requireNonNull(connectionQuotas);
    }

    public void update(ClientQuotasDelta quotasDelta) {
        quotasDelta.changes().forEach(this::update);
    }

    private void update(ClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        final Map<String, String> entries = entity.entries();
        if (entries.containsKey(ClientQuotaEntity.IP)) {
            // In the IP quota manager, empty Optional is used for the default entity
            final QuotaEntity ipEntity = Optional.ofNullable(entries.get(ClientQuotaEntity.IP))
                .map(QuotaEntity.IpEntity::new)
                .orElse(QuotaEntity.DefaultIpEntity.get());
            handleIpQuota(ipEntity, quotaDelta);
            return;
        }

        if (entries.containsKey(ClientQuotaEntity.USER) || entries.containsKey(ClientQuotaEntity.CLIENT_ID)) {
            final String userVal = entries.get(ClientQuotaEntity.USER);          // may be null
            final String clientIdVal = entries.get(ClientQuotaEntity.CLIENT_ID); // may be null

            final QuotaEntity userClientEntity;
            final boolean hasUser = entries.containsKey(ClientQuotaEntity.USER);
            final boolean hasClient = entries.containsKey(ClientQuotaEntity.CLIENT_ID);

            if (hasUser && hasClient) {
                if (userVal == null && clientIdVal == null) {
                    userClientEntity = QuotaEntity.DefaultUserDefaultClientIdEntity.get();
                } else if (userVal == null) {
                    userClientEntity = new QuotaEntity.DefaultUserExplicitClientIdEntity(clientIdVal);
                } else if (clientIdVal == null) {
                    userClientEntity = new QuotaEntity.ExplicitUserDefaultClientIdEntity(userVal);
                } else {
                    userClientEntity = new QuotaEntity.ExplicitUserExplicitClientIdEntity(userVal, clientIdVal);
                }
            } else if (hasUser) {
                userClientEntity = (userVal == null)
                    ? QuotaEntity.DefaultUserEntity.get()
                    : new QuotaEntity.UserEntity(userVal);
            } else { // hasClient
                userClientEntity = (clientIdVal == null)
                    ? QuotaEntity.DefaultClientIdEntity.get()
                    : new QuotaEntity.ClientIdEntity(clientIdVal);
            }

            quotaDelta.changes().forEach((key, opt) -> handleUserClientQuotaChange(userClientEntity, key, toOptional(opt)));
            return;
        }

        log.warn("Ignoring unsupported quota entity {}", entity);
    }

    /**
     * Scala's Option[Double] → Java Optional<Double>
     */
    private static Optional<Double> toOptional(OptionalDouble od) {
        return (od != null && od.isPresent()) ? Optional.of(od.getAsDouble()) : Optional.empty();
    }

    void handleIpQuota(QuotaEntity ipEntity, ClientQuotaDelta quotaDelta) {
        final Optional<InetAddress> inetAddress;
        if (ipEntity instanceof QuotaEntity.IpEntity e) {
            try {
                inetAddress = Optional.of(InetAddress.getByName(e.ip()));
            } catch (UnknownHostException ex) {
                throw new IllegalArgumentException("Unable to resolve address " + e.ip(), ex);
            }
        } else if (ipEntity instanceof QuotaEntity.DefaultIpEntity) {
            inetAddress = Optional.empty();
        } else {
            throw new IllegalStateException("Should only handle IP quota entities here");
        }

        quotaDelta.changes().forEach((key, value) -> {
            if (!QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG.equals(key)) {
                log.warn("Ignoring unexpected quota key {} for entity {}", key, ipEntity);
                return;
            }
            try {
                Optional<Integer> v = toOptional(value).map(d -> (int) d.doubleValue());
                connectionQuotas.updateIpConnectionRateQuota(inetAddress.orElse(null), v);
            } catch (Throwable t) {
                log.error("Failed to update IP quota {}", ipEntity, t);
            }
        });
    }

    private void handleUserClientQuotaChange(QuotaEntity quotaEntity, String key, Optional<Double> newValue) {
        final ClientQuotaManager manager;
        switch (key) {
            case QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> manager = quotaManagers.fetch;
            case QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> manager = quotaManagers.produce;
            case QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> manager = quotaManagers.request;
            case QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG -> manager = quotaManagers.controllerMutation;
            default -> {
                log.warn("Ignoring unexpected quota key {} for entity {}", key, quotaEntity);
                return;
            }
        }

        var pair = transferToClientQuotaEntity(quotaEntity);
        Optional<ConfigEntity> userEntity = pair.first();
        Optional<ConfigEntity> clientEntity = pair.second();

        Optional<Quota> quotaValue = newValue.map(v -> new Quota(v, true));

        try {
            manager.updateQuota(userEntity, clientEntity, quotaValue);
        } catch (Throwable t) {
            log.error("Failed to update user-client quota {}", quotaEntity, t);
        }
    }

    /**
     * Tuple holder to avoid Pair dependency
     */
    private record Pair<F, S>(F first, S second) {
    }

    public static Pair<Optional<ConfigEntity>, Optional<ConfigEntity>> transferToClientQuotaEntity(QuotaEntity quotaEntity) {
        if (quotaEntity instanceof QuotaEntity.UserEntity e) {
            return new Pair<>(
                Optional.of(new ClientQuotaManager.UserEntity(Sanitizer.sanitize(e.user()))),
                Optional.empty());
        } else if (quotaEntity instanceof QuotaEntity.DefaultUserEntity) {
            return new Pair<>(Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), Optional.empty());
        } else if (quotaEntity instanceof QuotaEntity.ClientIdEntity e) {
            return new Pair<>(Optional.empty(), Optional.of(new ClientQuotaManager.ClientIdEntity(e.clientId())));
        } else if (quotaEntity instanceof QuotaEntity.DefaultClientIdEntity) {
            return new Pair<>(Optional.empty(), Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID));
        } else if (quotaEntity instanceof QuotaEntity.ExplicitUserExplicitClientIdEntity e) {
            return new Pair<>(
                Optional.of(new ClientQuotaManager.UserEntity(Sanitizer.sanitize(e.user()))),
                Optional.of(new ClientQuotaManager.ClientIdEntity(e.clientId())));
        } else if (quotaEntity instanceof QuotaEntity.ExplicitUserDefaultClientIdEntity e) {
            return new Pair<>(
                Optional.of(new ClientQuotaManager.UserEntity(Sanitizer.sanitize(e.user()))),
                Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID));
        } else if (quotaEntity instanceof QuotaEntity.DefaultUserExplicitClientIdEntity e) {
            return new Pair<>(
                Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                Optional.of(new ClientQuotaManager.ClientIdEntity(e.clientId())));
        } else if (quotaEntity instanceof QuotaEntity.DefaultUserDefaultClientIdEntity) {
            return new Pair<>(
                Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID));
        } else if (quotaEntity instanceof QuotaEntity.IpEntity || quotaEntity instanceof QuotaEntity.DefaultIpEntity) {
            throw new IllegalStateException("Should not see IP quota entities here");
        } else {
            throw new IllegalArgumentException("Unknown quota entity: " + quotaEntity);
        }
    }
}