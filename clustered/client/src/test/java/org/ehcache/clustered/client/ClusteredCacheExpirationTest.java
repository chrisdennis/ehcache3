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

package org.ehcache.clustered.client;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.TierStatistics;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;

@ExtendWith(PassthroughServer.class)
@PassthroughServer.ServerResource(name = "primary-server-resource", size = 64)
public class ClusteredCacheExpirationTest {

  private static final String CLUSTERED_CACHE = "clustered-cache";

  private TestTimeSource timeSource = new TestTimeSource();

  private StatisticsService statisticsService = new DefaultStatisticsService();

  private CacheManagerBuilder<CacheManager> cacheManagerBuilder(ExpiryPolicy<Object, Object> expiry) {
    return newCacheManagerBuilder()
        .using(statisticsService)
        .using(new TimeSourceConfiguration(timeSource))
        .withCache(CLUSTERED_CACHE, newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(6, MemoryUnit.MB)
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB)))
              .withExpiry(expiry)
            .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG)));
  }

  private ExpiryPolicy<Object, Object> oneSecondExpiration() {
    return ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(1));
  }

  @Test
  public void testGetExpirationPropagatedToHigherTiers(@Cluster URI clusterUri) throws CachePersistenceException {
    CacheManagerBuilder<CacheManager> clusteredCacheManagerBuilder = cacheManagerBuilder(oneSecondExpiration());
    try(PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c)).build(true)) {

      Map<String, TierStatistics> tierStatistics = statisticsService.getCacheStatistics(CLUSTERED_CACHE).getTierStatistics();
      TierStatistics onheap = tierStatistics.get("OnHeap");
      TierStatistics offheap = tierStatistics.get("OffHeap");

      Cache<Long, String> cache = cacheManager.getCache(CLUSTERED_CACHE, Long.class, String.class);
      for (long i = 0; i < 30; i++) {
        cache.put(i, "value"); // store on the cluster
        cache.get(i); // push it up on heap and offheap tier
      }

      assertThat(onheap.getMappings()).isEqualTo(10);
      assertThat(offheap.getMappings()).isEqualTo(20);

      timeSource.advanceTime(1500); // go after expiration

      for (long i = 0; i < 30; i++) {
        assertThat(cache.get(i)).isEqualTo(null); // the value should have expired
      }

      assertThat(onheap.getMappings()).isEqualTo(0);
      assertThat(offheap.getMappings()).isEqualTo(0);
    }
  }

  @Test
  public void testGetNoExpirationPropagatedToHigherTiers(@Cluster URI clusterUri) throws CachePersistenceException {
    CacheManagerBuilder<CacheManager> clusteredCacheManagerBuilder = cacheManagerBuilder(ExpiryPolicyBuilder.noExpiration());

    try(PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c)).build(true)) {

      Map<String, TierStatistics> tierStatistics = statisticsService.getCacheStatistics(CLUSTERED_CACHE).getTierStatistics();
      TierStatistics onheap = tierStatistics.get("OnHeap");
      TierStatistics offheap = tierStatistics.get("OffHeap");

      Cache<Long, String> cache = cacheManager.getCache(CLUSTERED_CACHE, Long.class, String.class);
      for (long i = 0; i < 30; i++) {
        cache.put(i, "value"); // store on the cluster
        cache.get(i); // push it up on heap and offheap tier
      }

      assertThat(onheap.getMappings()).isEqualTo(10);
      assertThat(offheap.getMappings()).isEqualTo(20);
    }
  }

  @Test
  public void testPutIfAbsentExpirationPropagatedToHigherTiers(@Cluster URI clusterUri) throws CachePersistenceException {
    CacheManagerBuilder<CacheManager> clusteredCacheManagerBuilder = cacheManagerBuilder(oneSecondExpiration());

    try(PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c)).build(true)) {
      Cache<Long, String> cache = cacheManager.getCache(CLUSTERED_CACHE, Long.class, String.class);
      cache.put(1L, "value"); // store on the cluster
      cache.putIfAbsent(1L, "newvalue"); // push it up on heap tier
      timeSource.advanceTime(1500); // go after expiration
      assertThat(cache.get(1L)).isEqualTo(null); // the value should have expired
    }
  }
}

