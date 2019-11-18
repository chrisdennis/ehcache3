/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.clustered.lock;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock.Hold;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Topology;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.terracotta.connection.Connection;
import org.terracotta.passthrough.IClusterControl;

import static org.ehcache.clustered.lock.VoltronReadWriteLockIntegrationTest.async;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.terracotta.connection.ConnectionFactory.connect;

@WithSimpleTerracottaCluster
@Topology(2)
@Execution(ExecutionMode.CONCURRENT)
public class VoltronReadWriteLockPassiveIntegrationTest extends ClusteredTests {

  @BeforeEach
  public void waitForPassive(@Cluster IClusterControl clusterControl) throws Exception {
    clusterControl.waitForRunningPassivesInStandby();
  }

  @Test
  public void testSingleThreadSingleClientInteraction(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, TestInfo testInfo) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      VoltronReadWriteLock lock = new VoltronReadWriteLock(client, testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new));

      Hold hold = lock.writeLock();

      clusterControl.terminateActive();
      clusterControl.startOneServer();

      hold.unlock();
    }
  }

  @Test
  public void testMultipleThreadsSingleConnection(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, TestInfo testInfo) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      final VoltronReadWriteLock lock = new VoltronReadWriteLock(client, testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new));

      Hold hold = lock.writeLock();

      Future<Void> waiter = async(() -> {
        lock.writeLock().unlock();
        return null;
      });

      assertThrows(TimeoutException.class, () -> waiter.get(100, TimeUnit.MILLISECONDS));

      clusterControl.terminateActive();
      clusterControl.startOneServer();

      assertThrows(TimeoutException.class, () -> waiter.get(100, TimeUnit.MILLISECONDS));

      hold.unlock();

      waiter.get();
    }
  }

  @Test
  public void testMultipleClients(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, TestInfo testInfo) throws Throwable {
    try (Connection clientA = connect(clusterUri, new Properties());
         Connection clientB = connect(clusterUri, new Properties())) {
      VoltronReadWriteLock lockA = new VoltronReadWriteLock(clientA, testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new));

      Hold hold = lockA.writeLock();

      Future<Void> waiter = async(() -> {
        new VoltronReadWriteLock(clientB, testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)).writeLock().unlock();
        return null;
      });

      assertThrows(TimeoutException.class, () -> waiter.get(100, TimeUnit.MILLISECONDS));

      clusterControl.terminateActive();
      clusterControl.startOneServer();

      assertThrows(TimeoutException.class, () -> waiter.get(100, TimeUnit.MILLISECONDS));

      hold.unlock();

      waiter.get();
    }
  }
}
