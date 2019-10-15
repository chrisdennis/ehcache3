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

package org.ehcache.clustered.replication;

import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Topology;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.builders.CacheManagerBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.terracotta.passthrough.IClusterControl;

import java.net.URI;
import java.util.Properties;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.terracotta.connection.ConnectionFactory.connect;

@WithSimpleTerracottaCluster @Topology(2)
@Execution(ExecutionMode.CONCURRENT)
public class BasicLifeCyclePassiveReplicationTest extends ClusteredTests {

  @BeforeEach
  public void startServers(@Cluster IClusterControl clusterControl) throws Exception {
    clusterControl.startAllServers();
    clusterControl.waitForRunningPassivesInStandby();
  }

  @Test
  public void testDestroyCacheManager(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> configBuilder = newCacheManagerBuilder().with(cluster(clusterUri.resolve("/destroy-CM"))
      .autoCreate(server -> server.defaultServerResource(serverResource)));
    PersistentCacheManager cacheManager1 = configBuilder.build(true);
    PersistentCacheManager cacheManager2 = configBuilder.build(true);

    cacheManager2.close();

    try {
      cacheManager2.destroy();
      fail("Exception expected");
    } catch (Exception e) {
      e.printStackTrace();
    }

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    cacheManager1.createCache("test", newCacheConfigurationBuilder(Long.class, String.class, heap(10).with(clusteredDedicated(10, MB))));
  }

  @Test
  public void testDestroyLockEntity(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl) throws Exception {
    VoltronReadWriteLock lock1 = new VoltronReadWriteLock(connect(clusterUri, new Properties()), "my-lock");
    VoltronReadWriteLock.Hold hold1 = lock1.tryReadLock();

    VoltronReadWriteLock lock2 = new VoltronReadWriteLock(connect(clusterUri, new Properties()), "my-lock");
    assertThat(lock2.tryWriteLock(), nullValue());

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    hold1.unlock();
  }

}
