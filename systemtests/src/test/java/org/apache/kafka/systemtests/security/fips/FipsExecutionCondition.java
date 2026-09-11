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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Gates FIPS tests on the test JVM's host actually running in FIPS mode, so they skip cleanly on
 * dev laptops and non-FIPS CI runners rather than failing (or, worse, passing against a faked
 * environment) because the host can't back up what the test claims to verify.
 */
public class FipsExecutionCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (!FipsFixture.isHostFipsEnabled()) {
            return ConditionEvaluationResult.disabled(
                "Skipped: this host is not running in FIPS mode (/proc/sys/crypto/fips_enabled "
                    + "is not '1'). These tests require a genuine FIPS-mode Linux host running "
                    + "the container runtime directly (not a macOS/Windows Docker Desktop or "
                    + "Podman machine VM) — build a UBI image first via "
                    + ":systemtests:buildSystemTestImage, then run on such a host. "
                    + "See systemtests/PLAN-ubi-fips.md Part C2/C3.");
        }
        return ConditionEvaluationResult.enabled("Host is running in FIPS mode");
    }
}
