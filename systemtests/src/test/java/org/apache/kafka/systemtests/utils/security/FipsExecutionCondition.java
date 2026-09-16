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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Backs {@link FipsEnabled}: enables a test only when {@link FipsUtils#isHostFipsEnabled()} holds,
 * so FIPS tests skip cleanly on developer laptops and non-FIPS CI runners rather than failing, or
 * worse, passing against an environment that cannot back up what they claim to verify.
 */
public class FipsExecutionCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        // JUnit asks for the class, for each @ClusterSystemTest template method and for every
        // cluster invocation generated from it. Only the invocations are leaf tests as far as
        // Gradle is concerned, so only those are disabled: skipping the class or the template
        // method leaves Gradle with "No tests found" when this suite is run on its own on a
        // non-FIPS host, while skipped invocations are reported and counted normally.
        boolean isInvocation = context.getParent().flatMap(ExtensionContext::getTestMethod).isPresent();
        if (!isInvocation) {
            return ConditionEvaluationResult.enabled("FIPS mode is checked per cluster invocation");
        }
        if (!FipsUtils.isHostFipsEnabled()) {
            return ConditionEvaluationResult.disabled(
                "Skipped: this host is not running in FIPS mode (" + FipsUtils.PROC_FIPS_ENABLED
                    + " is not '1'). These tests need a FIPS-mode Linux host running the container "
                    + "runtime directly, not a macOS/Windows VM-backed Docker or Podman, plus a "
                    + "FIPS-capable image selected with -PkafkaSystemTestsImage.");
        }
        return ConditionEvaluationResult.enabled("Host is running in FIPS mode");
    }
}
