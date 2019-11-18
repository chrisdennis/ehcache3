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
package org.ehcache.clustered;

import org.ehcache.Cache;
import org.ehcache.CacheIterationException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.store.ServerStoreProxyException;
import org.ehcache.clustered.testing.extension.TerracottaCluster.ClientLeaseLength;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.resilience.StoreAccessException;
import org.junit.jupiter.api.Test;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.passthrough.IClusterControl;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.LongStream.range;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@WithSimpleTerracottaCluster
@ClientLeaseLength(5)
public class IterationFailureBehaviorTest extends ClusteredTests {

  private static final int KEYS = 100;

  @Test
  public void testIteratorFailover(@Cluster URI clusterUri, @Cluster String serverResource, @Cluster IClusterControl clusterControl) throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
      = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/iterator-cm"))
        .autoCreate(server -> server.defaultServerResource(serverResource)));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      CacheConfiguration<Long, String> smallConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(1, MemoryUnit.MB))).build();

      Cache<Long, String> smallCache = cacheManager.createCache("small-cache", smallConfig);
      range(0, KEYS).forEach(k -> smallCache.put(k, Long.toString(k)));

      CacheConfiguration<Long, byte[]> largeConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, byte[].class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))).build();

      Cache<Long, byte[]> largeCache = cacheManager.createCache("large-cache", largeConfig);
      byte[] value = new byte[10 * 1024];
      range(0, KEYS).forEach(k -> {
        largeCache.put(k, value);
      });

      Map<Long, String> smallMap = new HashMap<>();

      Iterator<Cache.Entry<Long, String>> smallIterator = smallCache.iterator();
      Cache.Entry<Long, String> smallNext = smallIterator.next();
      smallMap.put(smallNext.getKey(), smallNext.getValue());

      Iterator<Cache.Entry<Long, byte[]>> largeIterator = largeCache.iterator();
      Cache.Entry<Long, byte[]> largeNext = largeIterator.next();
      assertThat(largeCache.get(largeNext.getKey()), notNullValue());

      clusterControl.terminateActive();

      //large iterator fails
      CacheIterationException e = assertThrows(CacheIterationException.class, () -> largeIterator.forEachRemaining(k -> {}));
      assertThat(e.getCause(), instanceOf(StoreAccessException.class));
      assertThat(e.getCause().getCause(), instanceOf(ServerStoreProxyException.class));
      assertThat(e.getCause().getCause().getCause(), instanceOf(ConnectionClosedException.class));

      //small iterator completes... it fetched the entire batch in one shot
      smallIterator.forEachRemaining(k -> smallMap.put(k.getKey(), k.getValue()));

      assertThat(smallMap, is(range(0, KEYS).boxed().collect(toMap(identity(), k -> Long.toString(k)))));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testIteratorReconnect(@Cluster URI clusterUri, @Cluster String serverResource, @Cluster IClusterControl clusterControl) throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
      = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/iterator-cm"))
        .autoCreate(server -> server.defaultServerResource(serverResource)));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      CacheConfiguration<Long, String> smallConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(1, MemoryUnit.MB))).build();

      Cache<Long, String> smallCache = cacheManager.createCache("small-cache", smallConfig);
      range(0, KEYS).forEach(k -> smallCache.put(k, Long.toString(k)));

      CacheConfiguration<Long, byte[]> largeConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, byte[].class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))).build();

      Cache<Long, byte[]> largeCache = cacheManager.createCache("large-cache", largeConfig);
      byte[] value = new byte[10 * 1024];
      range(0, KEYS).forEach(k -> {
        largeCache.put(k, value);
      });

      Map<Long, String> smallMap = new HashMap<>();

      Iterator<Cache.Entry<Long, String>> smallIterator = smallCache.iterator();
      Cache.Entry<Long, String> smallNext = smallIterator.next();
      smallMap.put(smallNext.getKey(), smallNext.getValue());

      Iterator<Cache.Entry<Long, byte[]>> largeIterator = largeCache.iterator();
      Cache.Entry<Long, byte[]> largeNext = largeIterator.next();
      assertThat(largeCache.get(largeNext.getKey()), notNullValue());

      clusterControl.terminateAllServers();
      Thread.sleep(10000);
      clusterControl.startAllServers();
      clusterControl.waitForActive();

      //large iterator fails
      CacheIterationException e = assertThrows(CacheIterationException.class, () -> largeIterator.forEachRemaining(k -> {}));
      assertThat(e.getCause(), instanceOf(StoreAccessException.class));
      assertThat(e.getCause().getCause(), instanceOf(ServerStoreProxyException.class));
      assertThat(e.getCause().getCause().getCause(), instanceOf(ConnectionClosedException.class));

      //small iterator completes... it fetched the entire batch in one shot
      smallIterator.forEachRemaining(k -> smallMap.put(k.getKey(), k.getValue()));

      assertThat(smallMap, is(range(0, KEYS).boxed().collect(toMap(identity(), k -> Long.toString(k)))));
    } finally {
      cacheManager.close();
    }
  }
}
