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

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.testing.extension.TerracottaCluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Topology;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.terracotta.passthrough.IClusterControl;

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * Test the effect of cache eviction during passive sync.
 */
@WithSimpleTerracottaCluster @Topology(2)
@TerracottaCluster.SystemProperty(key = "ehcache.sync.data.gets.threshold", value = "2")
public class OversizedCacheOpsPassiveTest extends ClusteredTests {
  private static final int MAX_PUTS = 3000;
  private static final int MAX_SWITCH_OVER = 3;
  private static final int PER_ELEMENT_SIZE = 256 * 1024;
  private static final int CACHE_SIZE_IN_MB = 2;
  private static final String LARGE_VALUE = buildLargeString();

  @BeforeEach
  public void waitForPassive(@Cluster IClusterControl clusterControl) throws Exception {
    clusterControl.waitForRunningPassivesInStandby();
  }

  @Test
  public void oversizedPuts(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/crud-cm"))
            .autoCreate(server -> server.defaultServerResource(serverResource)));
    CountDownLatch syncLatch = new CountDownLatch(2);

    CompletableFuture<Void> f1 = CompletableFuture.runAsync(() -> doPuts(clusteredCacheManagerBuilder, syncLatch));
    CompletableFuture<Void> f2 = CompletableFuture.runAsync(() -> doPuts(clusteredCacheManagerBuilder, syncLatch));

    syncLatch.await();
    for (int i = 0; i < MAX_SWITCH_OVER; i++) {
      clusterControl.terminateActive();
      clusterControl.waitForActive();
      clusterControl.startOneServer();
      clusterControl.waitForRunningPassivesInStandby();
      Thread.sleep(2000);
    }

    f1.get();
    f2.get();
  }

  private void doPuts(CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder,
                      CountDownLatch syncLatch) {
    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(CACHE_SIZE_IN_MB, MemoryUnit.MB)))
        .build();

      syncLatch.countDown();
      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
      for (long i = 0; i < MAX_PUTS; i++) {
        if (i % 1000 == 0) {
          // a small pause
          try {
            Thread.sleep(10);
          } catch (InterruptedException ignored) {
          }
        }
        cache.put(i, LARGE_VALUE);
      }
    }
  }

  private static String buildLargeString() {
    char[] filler = new char[PER_ELEMENT_SIZE];
    Arrays.fill(filler, '0');
    return new String(filler);
  }
}
