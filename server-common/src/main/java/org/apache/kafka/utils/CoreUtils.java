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

package org.apache.kafka.utils;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.network.SocketServerConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * General helper functions!
 *
 * Notes:
 * - Methods mirror the Scala version's behavior.
 * - Where Scala used call-by-name, this class uses Runnable/Supplier.
 */
public final class CoreUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreUtils.class);
    private static final InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();

    private CoreUtils() {}

    /**
     * Do the given action and log any exceptions thrown without rethrowing them.
     *
     * @param action   The action to execute.
     * @param logging  The logging instance to use for logging the thrown exception.
     * @param logLevel The log level to use for logging.
     */
    @SuppressWarnings("SameParameterValue")
    public static void swallow(Runnable action, Logger logging, Level logLevel) {
        try {
            action.run();
        } catch (Throwable e) {
            switch (logLevel) {
                case ERROR -> logging.error(e.getMessage(), e);
                case WARN  -> logging.warn(e.getMessage(), e);
                case INFO  -> logging.info(e.getMessage(), e);
                case DEBUG -> logging.debug(e.getMessage(), e);
                case TRACE -> logging.trace(e.getMessage(), e);
            }
        }
    }

    /** Overload with default WARN level (matches Scala default). */
    public static void swallow(Runnable action, Logger logging) {
        swallow(action, logging, Level.WARN);
    }

    /**
     * Recursively delete the list of files/directories and any subfiles (if any exist).
     */
    public static void delete(List<String> files) throws IOException {
        for (String f : files) {
            Utils.delete(new File(f));
        }
    }

    /**
     * Register the given mbean with the platform mbean server, unregistering any mbean that was there before.
     * Returns false on failure (logs error), true on success.
     */
    public static boolean registerMBean(Object mbean, String name) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            synchronized (mbs) {
                ObjectName objName = new ObjectName(name);
                if (mbs.isRegistered(objName)) {
                    mbs.unregisterMBean(objName);
                }
                mbs.registerMBean(mbean, objName);
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to register Mbean {}", name, e);
            return false;
        }
    }

    /** Execute the given function inside the lock. */
    public static <T> T inLock(Lock lock, Supplier<T> fun) {
        lock.lock();
        try {
            return fun.get();
        } finally {
            lock.unlock();
        }
    }

    public static <T> T inReadLock(ReadWriteLock lock, Supplier<T> fun) {
        return inLock(lock.readLock(), fun);
    }

    public static <T> T inWriteLock(ReadWriteLock lock, Supplier<T> fun) {
        return inLock(lock.writeLock(), fun);
    }

    /**
     * Parse listeners into Endpoints and validate uniqueness of names/ports.
     * requireDistinctPorts=true.
     */
    public static List<Endpoint> listenerListToEndPoints(
        List<String> listeners,
        Map<ListenerName, SecurityProtocol> securityProtocolMap
    ) {
        return listenerListToEndPoints(listeners, securityProtocolMap, true);
    }

    private static void checkDuplicateListenerPorts(List<Endpoint> endpoints, List<String> listenersRaw) {
        List<Integer> ports = endpoints.stream().map(Endpoint::port).toList();
        long distinctCount = ports.stream().distinct().count();
        if (distinctCount != ports.size()) {
            throw new IllegalArgumentException(
                "Each listener must have a different port, listeners: " + listenersRaw);
        }
    }

    /**
     * Parse listeners into Endpoints and validate:
     * - Each listener name unique
     * - Each port unique, unless exactly two endpoints share a port and one host is valid IPv4 and the other is valid IPv6.
     * - For unit tests, port 0 is ignored in duplicate checking.
     *
     * @param requireDistinctPorts if true, enforce uniqueness rule above
     */
    public static List<Endpoint> listenerListToEndPoints(
        List<String> listeners,
        Map<ListenerName, SecurityProtocol> securityProtocolMap,
        boolean requireDistinctPorts
    ) {
        List<Endpoint> endpoints;
        try {
            endpoints = new ArrayList<>(SocketServerConfigs.listenerListToEndPoints(listeners, securityProtocolMap));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Error creating broker listeners from '" + listeners + "': " + e.getMessage(), e);
        }

        // Validate distinct listener names
        List<String> names = endpoints.stream().map(Endpoint::listener).toList();
        long distinctNames = names.stream().distinct().count();
        if (distinctNames != endpoints.size()) {
            throw new IllegalArgumentException("Each listener must have a different name, listeners: " + listeners);
        }

        // Group by port (excluding 0 for tests)
        Map<Integer, List<Endpoint>> byPort = endpoints.stream()
            .filter(ep -> ep.port() != 0)
            .collect(Collectors.groupingBy(Endpoint::port));

        // Partition duplicates vs singles
        List<Map.Entry<Integer, List<Endpoint>>> duplicatePorts = byPort.entrySet().stream()
            .filter(e -> e.getValue().size() > 1)
            .sorted(Comparator.comparingInt(Map.Entry::getKey))
            .toList();

        if (!duplicatePorts.isEmpty()) {
            for (Map.Entry<Integer, List<Endpoint>> entry : duplicatePorts) {
                int port = entry.getKey();
                List<Endpoint> eps = entry.getValue();

                // Partition duplicates by whether the host is a valid IP (v4 or v6)
                List<Endpoint> withIpHost = new ArrayList<>();
                List<Endpoint> withoutIpHost = new ArrayList<>();
                for (Endpoint ep : eps) {
                    String host = ep.host();
                    if (host != null && inetAddressValidator.isValid(host)) {
                        withIpHost.add(ep);
                    } else {
                        withoutIpHost.add(ep);
                    }
                }

                if (requireDistinctPorts) {
                    // If any duplicate entries lack a valid IP, this is invalid outright
                    if (!withoutIpHost.isEmpty()) {
                        checkDuplicateListenerPorts(withoutIpHost, listeners);
                    }
                }

                if (!withIpHost.isEmpty()) {
                    // Only the special case of exactly two endpoints on the same port is allowed:
                    // one IPv4 and one IPv6
                    if (withIpHost.size() == 2) {
                        Endpoint ep1 = withIpHost.get(0);
                        Endpoint ep2 = withIpHost.get(1);
                        String h1 = ep1.host();
                        String h2 = ep2.host();

                        boolean oneV4otherV6 =
                            (inetAddressValidator.isValidInet4Address(h1) && inetAddressValidator.isValidInet6Address(h2)) ||
                                (inetAddressValidator.isValidInet6Address(h1) && inetAddressValidator.isValidInet4Address(h2));

                        String errorMessage = "If you have two listeners on the same port then one needs to be IPv4 " +
                            "and the other IPv6, listeners: " + listeners + ", port: " + port;

                        if (requireDistinctPorts && !oneV4otherV6) {
                            throw new IllegalArgumentException(errorMessage);
                        }

                        // Even if the two-IP case is valid, having any *additional* duplicates on that port is invalid.
                        if (requireDistinctPorts && !withoutIpHost.isEmpty()) {
                            throw new IllegalArgumentException(errorMessage);
                        }
                    } else {
                        // More than two duplicates on the same port cannot be justified by IPv4/v6 pair rule
                        if (requireDistinctPorts) {
                            throw new IllegalArgumentException(
                                "Each listener must have a different port unless exactly one listener has an IPv4 address " +
                                    "and the other IPv6 address, listeners: " + listeners + ", port: " + port);
                        }
                    }
                }
            }
        }

        return Collections.unmodifiableList(endpoints);
    }
}