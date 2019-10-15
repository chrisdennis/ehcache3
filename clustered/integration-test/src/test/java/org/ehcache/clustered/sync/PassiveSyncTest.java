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

package org.ehcache.clustered.sync;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Topology;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.terracotta.passthrough.IClusterControl;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@WithSimpleTerracottaCluster @Topology(2)
public class PassiveSyncTest extends ClusteredTests {

  @BeforeEach
  public void startServers(@Cluster IClusterControl clusterControl) throws Exception {
    clusterControl.terminateOnePassive();
  }

  @Test @Timeout(150)
  public void testSync(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource) throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
      = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/op-sync"))
        .autoCreate(server -> server.defaultServerResource(serverResource)));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(1, MemoryUnit.MB))).build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

      for (long i = -5; i < 5; i++) {
        cache.put(i, "value" + i);
      }

      clusterControl.startOneServer();
      clusterControl.waitForRunningPassivesInStandby();
      clusterControl.terminateActive();
      clusterControl.waitForActive();

      for (long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(130); cache.get(0L) == null && System.nanoTime() < end; ) {
        Thread.sleep(100);
      }

      for (long i = -5; i < 5; i++) {
        assertThat(cache.get(i), equalTo("value" + i));
      }
    } finally {
      cacheManager.close();
    }
  }

  @Disabled
  @Test
  public void testLifeCycleOperationsOnSync(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource) throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
      = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/lifecycle-sync"))
        .autoCreate(server -> server.defaultServerResource(serverResource)));

    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(1, MemoryUnit.MB))).build();

      final Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

      for (long i = 0; i < 100; i++) {
        cache.put(i, "value" + i);
      }

      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicBoolean complete = new AtomicBoolean(false);
      Thread lifeCycleThread = new Thread(() -> {
        while (!complete.get()) {
          try {
            latch.await();
            clusteredCacheManagerBuilder.build(true);
            Thread.sleep(200);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
      lifeCycleThread.start();
      clusterControl.startOneServer();
      latch.countDown();
      clusterControl.waitForRunningPassivesInStandby();
      clusterControl.terminateActive();
      complete.set(true);

      for (long i = 0; i < 100; i++) {
        assertThat(cache.get(i), equalTo("value" + i));
      }
    }
  }
}
