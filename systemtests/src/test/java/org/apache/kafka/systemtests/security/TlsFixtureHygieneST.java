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

package org.apache.kafka.systemtests.security;

import org.apache.kafka.systemtests.utils.security.TlsFixture;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is the ducktape-MD5 (KAFKA-20929) lesson applied to the new framework, run on any JVM.
 * The fixture should never hand out a JKS store or an MD5/SHA-1-signed certificate, since those
 * are exactly the kind of thing that quietly breaks under a FIPS-restricted provider list.
 */
class TlsFixtureHygieneST {

    private static final List<String> APPROVED_SIGNATURE_ALGORITHMS = List.of("SHA256withRSA", "SHA384withRSA", "SHA512withRSA");

    @Tag("system")
    @Test
    void generatedStoresAreNotJksAndCertsAreNotWeaklySigned() throws Exception {
        TlsFixture tls = TlsFixture.generate();

        assertFalse("JKS".equalsIgnoreCase(TlsFixture.STORE_TYPE_PKCS12),
            "TlsFixture's default store type constant must not be JKS");
        assertEquals("PKCS12", TlsFixture.STORE_TYPE_PKCS12);

        assertApprovedSignature(tls.caCertificate());

        KeyStore trustStore = KeyStore.getInstance(TlsFixture.STORE_TYPE_PKCS12);
        trustStore.load(new ByteArrayInputStream(tls.trustStoreBytes(TlsFixture.STORE_TYPE_PKCS12)), TlsFixture.STORE_PASSWORD);
        assertEquals(TlsFixture.STORE_TYPE_PKCS12, trustStore.getType());

        KeyStore serverStore = KeyStore.getInstance(TlsFixture.STORE_TYPE_PKCS12);
        serverStore.load(new ByteArrayInputStream(tls.serverKeyStoreBytes(TlsFixture.STORE_TYPE_PKCS12, "kafka-0", "localhost")),
            TlsFixture.STORE_PASSWORD);
        assertEquals(TlsFixture.STORE_TYPE_PKCS12, serverStore.getType());
        X509Certificate serverCert = (X509Certificate) serverStore.getCertificateChain("cert")[0];
        assertApprovedSignature(serverCert);
    }

    private static void assertApprovedSignature(X509Certificate cert) {
        boolean approved = APPROVED_SIGNATURE_ALGORITHMS.stream()
            .anyMatch(alg -> alg.equalsIgnoreCase(cert.getSigAlgName()));
        assertTrue(approved,
            "Certificate " + cert.getSubjectX500Principal() + " uses " + cert.getSigAlgName()
                + ", not one of the FIPS-approved algorithms " + APPROVED_SIGNATURE_ALGORITHMS);
    }
}
