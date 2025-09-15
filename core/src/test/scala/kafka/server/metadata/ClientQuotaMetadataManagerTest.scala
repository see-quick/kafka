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
package kafka.server.metadata

import org.apache.kafka.image.ClientQuotaDelta
import org.apache.kafka.server.quota.ClientQuotaManager
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.Optional

class ClientQuotaMetadataManagerTest {

  @Test
  def testHandleIpQuota(): Unit = {
    val manager = new ClientQuotaMetadataManagerImpl(null, null)
    assertThrows(classOf[IllegalArgumentException], () => manager.handleIpQuota(new ClientQuotaMetadataManagerImpl.IpEntity("a"), new ClientQuotaDelta(null)))
    assertThrows(classOf[IllegalStateException], () => manager.handleIpQuota(new ClientQuotaMetadataManagerImpl.UserEntity("a"), new ClientQuotaDelta(null)))
    assertDoesNotThrow { new Executable { def execute(): Unit = manager.handleIpQuota(new ClientQuotaMetadataManagerImpl.DefaultIpEntity(), new ClientQuotaDelta(null)) } }
    assertDoesNotThrow { new Executable { def execute(): Unit = manager.handleIpQuota(new ClientQuotaMetadataManagerImpl.IpEntity("192.168.1.1"), new ClientQuotaDelta(null)) } }
    assertDoesNotThrow { new Executable { def execute(): Unit = manager.handleIpQuota(new ClientQuotaMetadataManagerImpl.IpEntity("2001:db8::1"), new ClientQuotaDelta(null)) } }
  }

  @Test
  def testTransferToClientQuotaEntity(): Unit = {
    
    assertThrows(classOf[IllegalStateException],() => ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.IpEntity("a")))
    assertThrows(classOf[IllegalStateException],() => ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.DefaultIpEntity()))
    val result = ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.UserEntity("user"))
    assertEquals(Optional.of(new ClientQuotaManager.UserEntity("user")), result.getKey)
    assertEquals(Optional.empty(), result.getValue)
    val result2 = ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.DefaultUserEntity())
    assertEquals(Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), result2.getKey)
    assertEquals(Optional.empty(), result2.getValue)
    val result3 = ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.ClientIdEntity("client"))
    assertEquals(Optional.empty(), result3.getKey)
    assertEquals(Optional.of(new ClientQuotaManager.ClientIdEntity("client")), result3.getValue)
    val result4 = ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.DefaultClientIdEntity())
    assertEquals(Optional.empty(), result4.getKey)
    assertEquals(Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID), result4.getValue)
    val result5 = ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.ExplicitUserExplicitClientIdEntity("user", "client"))
    assertEquals(Optional.of(new ClientQuotaManager.UserEntity("user")), result5.getKey)
    assertEquals(Optional.of(new ClientQuotaManager.ClientIdEntity("client")), result5.getValue)
    val result6 = ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.ExplicitUserDefaultClientIdEntity("user"))
    assertEquals(Optional.of(new ClientQuotaManager.UserEntity("user")), result6.getKey)
    assertEquals(Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID), result6.getValue)
    val result7 = ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.DefaultUserExplicitClientIdEntity("client"))
    assertEquals(Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), result7.getKey)
    assertEquals(Optional.of(new ClientQuotaManager.ClientIdEntity("client")), result7.getValue)
    val result8 = ClientQuotaMetadataManagerImpl.transferToClientQuotaEntity(new ClientQuotaMetadataManagerImpl.DefaultUserDefaultClientIdEntity())
    assertEquals(Optional.of(ClientQuotaManager.DEFAULT_USER_ENTITY), result8.getKey)
    assertEquals(Optional.of(ClientQuotaManager.DEFAULT_USER_CLIENT_ID), result8.getValue)
  }
}