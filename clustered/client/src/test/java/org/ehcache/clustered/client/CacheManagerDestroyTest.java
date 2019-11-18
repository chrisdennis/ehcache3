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
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.Status;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.client.internal.PassthroughServer.OffHeapResource;
import org.ehcache.clustered.client.internal.PassthroughServer.WithSimplePassthroughServer;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@WithSimplePassthroughServer
public class CacheManagerDestroyTest {

  @Test
  public void testDestroyCacheManagerWithSingleClient(@Cluster URI clusterUri) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = newCacheManagerBuilder().with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c)).build(true);

    persistentCacheManager.close();
    persistentCacheManager.destroy();

    assertThat(persistentCacheManager.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testCreateDestroyCreate(@Cluster URI clusterUri, @Cluster String resource) throws Exception {
    PersistentCacheManager cacheManager = newCacheManagerBuilder().with(cluster(clusterUri.resolve("/cache-manager"))
      .autoCreate(c -> c.defaultServerResource(resource)))
      .withCache("my-cache", newCacheConfigurationBuilder(Long.class, String.class, heap(10).with(ClusteredResourcePoolBuilder
        .clusteredDedicated(2, MemoryUnit.MB))))
      .build(true);

    cacheManager.close();
    cacheManager.destroy();

    cacheManager.init();
  }

  @Test
  public void testDestroyCacheManagerWithMultipleClients(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager1 = newCacheManagerBuilder().with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);
    PersistentCacheManager persistentCacheManager2 = newCacheManagerBuilder().with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);

    persistentCacheManager1.close();

    StateTransitionException e = assertThrows(StateTransitionException.class, () -> persistentCacheManager1.destroy());
    assertThat(e.getMessage(), is("Couldn't acquire cluster-wide maintenance lease"));

    assertThat(persistentCacheManager1.getStatus(), is(Status.UNINITIALIZED));

    assertThat(persistentCacheManager2.getStatus(), is(Status.AVAILABLE));

    Cache<Long, String> cache = persistentCacheManager2.createCache("test", newCacheConfigurationBuilder(Long.class, String.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredDedicated(2, MemoryUnit.MB))));

    cache.put(1L, "One");

    assertThat(cache.get(1L), is("One"));

    persistentCacheManager2.close();
  }

  @Test
  public void testDestroyCacheManagerDoesNotAffectsExistingCacheWithExistingClientsConnected(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {

    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder = newCacheManagerBuilder().with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource)))
      .withCache("test", newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(2, MemoryUnit.MB))));

    PersistentCacheManager persistentCacheManager1 = cacheManagerBuilder.build(true);
    PersistentCacheManager persistentCacheManager2 = cacheManagerBuilder.build(true);

    persistentCacheManager1.close();
    StateTransitionException e = assertThrows(StateTransitionException.class, () -> persistentCacheManager1.destroy());
    assertThat(e.getMessage(), is("Couldn't acquire cluster-wide maintenance lease"));

    Cache<Long, String> cache = persistentCacheManager2.getCache("test", Long.class, String.class);

    cache.put(1L, "One");

    assertThat(cache.get(1L), is("One"));

    persistentCacheManager2.close();
  }

  @Test
  public void testCloseCacheManagerSingleClient(@Cluster URI clusterUri, @Cluster String resource) {
    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder = newCacheManagerBuilder().with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource)))
        .withCache("test", newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.clusteredDedicated(2, MemoryUnit.MB))));

    PersistentCacheManager persistentCacheManager1 = cacheManagerBuilder.build(true);

    persistentCacheManager1.close();

    persistentCacheManager1.init();

    Cache<Long, String> cache = persistentCacheManager1.getCache("test", Long.class, String.class);
    cache.put(1L, "One");

    assertThat(cache.get(1L), is("One"));

    persistentCacheManager1.close();
  }

  @Test
  public void testCloseCacheManagerMultipleClients(@Cluster URI clusterUri, @Cluster String resource) {
    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder = newCacheManagerBuilder().with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource)))
        .withCache("test", newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.clusteredDedicated(2, MemoryUnit.MB))));

    PersistentCacheManager persistentCacheManager1 = cacheManagerBuilder.build(true);
    PersistentCacheManager persistentCacheManager2 = cacheManagerBuilder.build(true);

    Cache<Long, String> cache = persistentCacheManager1.getCache("test", Long.class, String.class);
    cache.put(1L, "One");

    assertThat(cache.get(1L), is("One"));

    persistentCacheManager1.close();
    assertThat(persistentCacheManager1.getStatus(), is(Status.UNINITIALIZED));

    Cache<Long, String> cache2 = persistentCacheManager2.getCache("test", Long.class, String.class);

    assertThat(cache2.get(1L), is("One"));

    persistentCacheManager2.close();
  }

}
