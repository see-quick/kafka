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

package org.apache.kafka.systemtests.security.fips;

import java.security.KeyFactory;
import java.security.MessageDigest;

/**
 * Standalone probe (no Kafka classes, no external dependencies, no nested/lambda classes so a
 * single {@code .class} file is sufficient to copy into a container) run directly inside a
 * FIPS-activated UBI container via {@code java -cp}, pinning the exact KAFKA-20997 regression
 * without booting a broker: under Red Hat FIPS, {@code KeyFactory.getInstance("DSA")} throws
 * while RSA key factories and SHA-256 digests keep working (see PLAN-ubi-fips.md C2c).
 *
 * <p>Prints one {@code KEY=value} line per check so the caller can assert on stdout without
 * relying on exit codes for individual checks.
 */
public final class FipsDsaProbe {

    private FipsDsaProbe() {
    }

    public static void main(String[] args) {
        System.out.println("DSA_KEYFACTORY=" + probeDsaKeyFactory());
        System.out.println("RSA_KEYFACTORY=" + probeRsaKeyFactory());
        System.out.println("SHA256_DIGEST=" + probeSha256Digest());
    }

    private static String probeDsaKeyFactory() {
        try {
            KeyFactory.getInstance("DSA");
            return "AVAILABLE";
        } catch (Exception e) {
            return "UNAVAILABLE:" + e.getClass().getSimpleName();
        }
    }

    private static String probeRsaKeyFactory() {
        try {
            KeyFactory.getInstance("RSA");
            return "AVAILABLE";
        } catch (Exception e) {
            return "UNAVAILABLE:" + e.getClass().getSimpleName();
        }
    }

    private static String probeSha256Digest() {
        try {
            MessageDigest.getInstance("SHA-256");
            return "AVAILABLE";
        } catch (Exception e) {
            return "UNAVAILABLE:" + e.getClass().getSimpleName();
        }
    }
}
