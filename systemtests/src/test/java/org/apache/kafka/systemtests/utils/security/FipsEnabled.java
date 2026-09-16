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

package org.apache.kafka.systemtests.utils.security;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test class (or method) as requiring a host that is genuinely running in FIPS mode.
 *
 * <p>Carries everything the FIPS tier needs so the test itself only declares its cluster: the
 * {@code fips} tag the Gradle {@code systemTest} task excludes unless {@code -PkafkaSystemtestsFips=true}
 * is passed, and the {@link FipsExecutionCondition} that skips the test with an explanation on any
 * host whose kernel does not report {@code /proc/sys/crypto/fips_enabled=1}. Nothing is simulated:
 * a test annotated with this either runs against real FIPS or does not run.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Tag("fips")
@ExtendWith(FipsExecutionCondition.class)
public @interface FipsEnabled {
}
