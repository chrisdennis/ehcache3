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
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.client.internal.PassthroughServer.WithSimplePassthroughServer;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.time.Duration;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@WithSimplePassthroughServer
public class BasicClusteredCacheExpiryTest {

  private static final CacheManagerBuilder<CacheManager> BASE_BUILDER =
      newCacheManagerBuilder()
          .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated(2, MemoryUnit.MB)))
              .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1L)))
              .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG)));

  @Test
  public void testGetExpiredSingleClient(@Cluster URI clusterURI, @Cluster String resource) {
    TestTimeSource timeSource = new TestTimeSource();
    TimeSourceConfiguration timeSourceConfiguration = new TimeSourceConfiguration(timeSource);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        BASE_BUILDER.with(cluster(clusterURI).autoCreate(c -> c.defaultServerResource(resource))).using(timeSourceConfiguration);

    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    final Cache<Long, String> cache = cacheManager.getCache("clustered-cache", Long.class, String.class);

    cache.put(1L, "value");
    assertThat(cache.get(1L), is("value"));

    timeSource.advanceTime(1);

    assertThat(cache.get(1L), nullValue());

    cacheManager.close();

  }

  @Test
  public void testGetExpiredTwoClients(@Cluster URI clusterURI, @Cluster String resource) {

    TestTimeSource timeSource = new TestTimeSource();
    TimeSourceConfiguration timeSourceConfiguration = new TimeSourceConfiguration(timeSource);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        BASE_BUILDER.with(cluster(clusterURI).autoCreate(c -> c.defaultServerResource(resource))).using(timeSourceConfiguration);

    final PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true);
    final PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true);

    final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
    final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

    assertThat(cache2.get(1L), nullValue());
    cache1.put(1L, "value1");
    assertThat(cache1.get(1L), is("value1"));
    timeSource.advanceTime(1L);

    assertThat(cache2.get(1L), nullValue());
    assertThat(cache1.get(1L), nullValue());

    cacheManager2.close();
    cacheManager1.close();
  }

  @Test
  public void testContainsKeyExpiredTwoClients(@Cluster URI clusterURI, @Cluster String resource) {

    TestTimeSource timeSource = new TestTimeSource();
    TimeSourceConfiguration timeSourceConfiguration = new TimeSourceConfiguration(timeSource);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        BASE_BUILDER.with(cluster(clusterURI).autoCreate(c -> c.defaultServerResource(resource))).using(timeSourceConfiguration);

    final PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true);
    final PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true);

    final Cache<Long, String> cache1 = cacheManager1.getCache("clustered-cache", Long.class, String.class);
    final Cache<Long, String> cache2 = cacheManager2.getCache("clustered-cache", Long.class, String.class);

    assertThat(cache2.get(1L), nullValue());
    cache1.put(1L, "value1");
    assertThat(cache1.containsKey(1L), is(true));
    timeSource.advanceTime(1L);

    assertThat(cache1.containsKey(1L), is(false));
    assertThat(cache2.containsKey(1L), is(false));

    cacheManager2.close();
    cacheManager1.close();

  }

}
