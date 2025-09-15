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
package kafka.server.metadata;

import kafka.network.ConnectionQuotas;
import kafka.server.QuotaFactory.QuotaManagers;

import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.image.ClientQuotaDelta;
import org.apache.kafka.image.ClientQuotasDelta;
import org.apache.kafka.metadata.publisher.ClientQuotaMetadataManager;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.quota.ClientQuotaEntity.ConfigEntity;
import org.apache.kafka.server.quota.ClientQuotaManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import scala.Option;

/**
 * Process quota metadata records as they appear in the metadata log and update quota managers and cache as necessary.
 *
 * This is the Java implementation that replaces the Scala version in kafka.server.metadata.ClientQuotaMetadataManager.
 */
public class ClientQuotaMetadataManagerImpl implements ClientQuotaMetadataManager {

    private static final Logger log = LoggerFactory.getLogger(ClientQuotaMetadataManagerImpl.class);

    // A strict hierarchy of entities that we support
    public sealed interface QuotaEntity permits IpEntity, DefaultIpEntity, UserEntity, DefaultUserEntity,
            ClientIdEntity, DefaultClientIdEntity, ExplicitUserExplicitClientIdEntity,
            ExplicitUserDefaultClientIdEntity, DefaultUserExplicitClientIdEntity, DefaultUserDefaultClientIdEntity {
    }

    public record IpEntity(String ip) implements QuotaEntity { }
    public record DefaultIpEntity() implements QuotaEntity { }
    public record UserEntity(String user) implements QuotaEntity { }
    public record DefaultUserEntity() implements QuotaEntity { }
    public record ClientIdEntity(String clientId) implements QuotaEntity { }
    public record DefaultClientIdEntity() implements QuotaEntity { }
    public record ExplicitUserExplicitClientIdEntity(String user, String clientId) implements QuotaEntity { }
    public record ExplicitUserDefaultClientIdEntity(String user) implements QuotaEntity { }
    public record DefaultUserExplicitClientIdEntity(String clientId) implements QuotaEntity { }
    public record DefaultUserDefaultClientIdEntity() implements QuotaEntity { }

    private final QuotaManagers quotaManagers;
    private final ConnectionQuotas connectionQuotas;

    public ClientQuotaMetadataManagerImpl(QuotaManagers quotaManagers, ConnectionQuotas connectionQuotas) {
        this.quotaManagers = quotaManagers;
        this.connectionQuotas = connectionQuotas;
    }

    /**
     * Update quota managers based on the provided client quotas delta.
     *
     * @param quotasDelta the delta containing client quota changes
     */
    @Override
    public void update(ClientQuotasDelta quotasDelta) {
        quotasDelta.changes().forEach(this::update);
    }

    private void update(ClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        if (entity.entries().containsKey(ClientQuotaEntity.IP)) {
            // In the IP quota manager, null is used for default entity
            QuotaEntity ipEntity;
            String ip = entity.entries().get(ClientQuotaEntity.IP);
            if (ip != null) {
                ipEntity = new IpEntity(ip);
            } else {
                ipEntity = new DefaultIpEntity();
            }
            handleIpQuota(ipEntity, quotaDelta);
        } else if (entity.entries().containsKey(ClientQuotaEntity.USER) ||
                entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)) {
            // These values may be null, which is why we needed to use containsKey.
            String userVal = entity.entries().get(ClientQuotaEntity.USER);
            String clientIdVal = entity.entries().get(ClientQuotaEntity.CLIENT_ID);

            // In User+Client quota managers, "<default>" is used for default entity, so we need to represent all possible
            // combinations of values, defaults, and absent entities
            QuotaEntity userClientEntity;
            if (entity.entries().containsKey(ClientQuotaEntity.USER) &&
                    entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)) {
                if (userVal == null && clientIdVal == null) {
                    userClientEntity = new DefaultUserDefaultClientIdEntity();
                } else if (userVal == null) {
                    userClientEntity = new DefaultUserExplicitClientIdEntity(clientIdVal);
                } else if (clientIdVal == null) {
                    userClientEntity = new ExplicitUserDefaultClientIdEntity(userVal);
                } else {
                    userClientEntity = new ExplicitUserExplicitClientIdEntity(userVal, clientIdVal);
                }
            } else if (entity.entries().containsKey(ClientQuotaEntity.USER)) {
                if (userVal == null) {
                    userClientEntity = new DefaultUserEntity();
                } else {
                    userClientEntity = new UserEntity(userVal);
                }
            } else {
                if (clientIdVal == null) {
                    userClientEntity = new DefaultClientIdEntity();
                } else {
                    userClientEntity = new ClientIdEntity(clientIdVal);
                }
            }
            quotaDelta.changes().forEach((key, value) ->
                    handleUserClientQuotaChange(userClientEntity, key, value == null ? null : value.getAsDouble()));
        } else {
            log.warn("Ignoring unsupported quota entity {}", entity);
        }
    }

    public void handleIpQuota(QuotaEntity ipEntity, ClientQuotaDelta quotaDelta) {
        Option<InetAddress> inetAddress;
        if (ipEntity instanceof IpEntity ipEnt) {
            try {
                inetAddress = Option.apply(InetAddress.getByName(ipEnt.ip()));
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Unable to resolve address " + ipEnt.ip(), e);
            }
        } else if (ipEntity instanceof DefaultIpEntity) {
            inetAddress = Option.empty();
        } else {
            throw new IllegalStateException("Should only handle IP quota entities here");
        }

        quotaDelta.changes().forEach((key, value) -> {
            // The connection quota only understands the connection rate limit
            if (!key.equals(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG)) {
                log.warn("Ignoring unexpected quota key {} for entity {}", key, ipEntity);
            } else {
                try {
                    Option<Object> scalaRate = value == null ? Option.empty() : Option.apply(Integer.valueOf((int) value.getAsDouble()));
                    connectionQuotas.updateIpConnectionRateQuota(inetAddress, scalaRate);
                } catch (Exception t) {
                    log.error("Failed to update IP quota {}", ipEntity, t);
                }
            }
        });
    }

    private void handleUserClientQuotaChange(QuotaEntity quotaEntity, String key, Double newValue) {
        ClientQuotaManager manager;
        switch (key) {
            case QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG:
                manager = quotaManagers.fetch();
                break;
            case QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG:
                manager = quotaManagers.produce();
                break;
            case QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG:
                manager = quotaManagers.request();
                break;
            case QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG:
                manager = quotaManagers.controllerMutation();
                break;
            default:
                log.warn("Ignoring unexpected quota key {} for entity {}", key, quotaEntity);
                manager = null;
                break;
        }

        if (manager == null) {
            return;
        }

        // Convert entity into Options with sanitized values for QuotaManagers
        var entityPair = transferToClientQuotaEntity(quotaEntity);
        Optional<ConfigEntity> userEntity = entityPair.getKey();
        Optional<ConfigEntity> clientEntity = entityPair.getValue();
        Optional<Quota> quotaValue = newValue == null ? Optional.empty() : Optional.of(new Quota(newValue, true));

        try {
            manager.updateQuota(userEntity, clientEntity, quotaValue);
        } catch (Exception t) {
            log.error("Failed to update user-client quota {}", quotaEntity, t);
        }
    }

    public static java.util.AbstractMap.SimpleEntry<Optional<ConfigEntity>, Optional<ConfigEntity>>
            transferToClientQuotaEntity(QuotaEntity quotaEntity) {
        if (quotaEntity instanceof UserEntity) {
            UserEntity userEntity = (UserEntity) quotaEntity;
            return new java.util.AbstractMap.SimpleEntry<>(
                    Optional.of(new ClientQuotaManager.UserEntity(Sanitizer.sanitize(userEntity.user()))),
                    Optional.empty());
        } else if (quotaEntity instanceof DefaultUserEntity) {
            return new java.util.AbstractMap.SimpleEntry<>(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.empty());
        } else if (quotaEntity instanceof ClientIdEntity) {
            ClientIdEntity clientIdEntity = (ClientIdEntity) quotaEntity;
            return new java.util.AbstractMap.SimpleEntry<>(
                    Optional.empty(),
                    Optional.of(new ClientQuotaManager.ClientIdEntity(clientIdEntity.clientId())));
        } else if (quotaEntity instanceof DefaultClientIdEntity) {
            return new java.util.AbstractMap.SimpleEntry<>(
                    Optional.empty(),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID));
        } else if (quotaEntity instanceof ExplicitUserExplicitClientIdEntity) {
            ExplicitUserExplicitClientIdEntity explicitUserExplicitClientIdEntity = (ExplicitUserExplicitClientIdEntity) quotaEntity;
            return new java.util.AbstractMap.SimpleEntry<>(
                    Optional.of(new ClientQuotaManager.UserEntity(Sanitizer.sanitize(explicitUserExplicitClientIdEntity.user()))),
                    Optional.of(new ClientQuotaManager.ClientIdEntity(explicitUserExplicitClientIdEntity.clientId())));
        } else if (quotaEntity instanceof ExplicitUserDefaultClientIdEntity) {
            ExplicitUserDefaultClientIdEntity explicitUserDefaultClientIdEntity = (ExplicitUserDefaultClientIdEntity) quotaEntity;
            return new java.util.AbstractMap.SimpleEntry<>(
                    Optional.of(new ClientQuotaManager.UserEntity(Sanitizer.sanitize(explicitUserDefaultClientIdEntity.user()))),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID));
        } else if (quotaEntity instanceof DefaultUserExplicitClientIdEntity) {
            DefaultUserExplicitClientIdEntity defaultUserExplicitClientIdEntity = (DefaultUserExplicitClientIdEntity) quotaEntity;
            return new java.util.AbstractMap.SimpleEntry<>(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.of(new ClientQuotaManager.ClientIdEntity(defaultUserExplicitClientIdEntity.clientId())));
        } else if (quotaEntity instanceof DefaultUserDefaultClientIdEntity) {
            return new java.util.AbstractMap.SimpleEntry<>(
                    Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY),
                    Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID));
        } else if (quotaEntity instanceof IpEntity || quotaEntity instanceof DefaultIpEntity) {
            throw new IllegalStateException("Should not see IP quota entities here");
        } else {
            throw new IllegalArgumentException("Unknown quota entity type: " + quotaEntity.getClass());
        }
    }
}