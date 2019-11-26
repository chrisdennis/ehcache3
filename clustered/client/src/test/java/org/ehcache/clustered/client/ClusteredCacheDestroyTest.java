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
import org.ehcache.Status;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.client.internal.PassthroughServer.OffHeapResource;
import org.ehcache.clustered.client.internal.PassthroughServer.WithSimplePassthroughServer;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clustered;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@WithSimplePassthroughServer
public class ClusteredCacheDestroyTest {

  private static final String CLUSTERED_CACHE = "clustered-cache";

  private static final CacheManagerBuilder<CacheManager> BASE_BUILDER =
      newCacheManagerBuilder().withCache(CLUSTERED_CACHE, newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated(8, MemoryUnit.MB)))
              .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG)));

  @Test
  public void testDestroyCacheWhenSingleClientIsConnected(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager"))
      .autoCreate(c -> c.defaultServerResource(resource))).build(true);

    persistentCacheManager.destroyCache(CLUSTERED_CACHE);

    final Cache<Long, String> cache = persistentCacheManager.getCache(CLUSTERED_CACHE, Long.class, String.class);

    assertThat(cache, nullValue());

    persistentCacheManager.close();
  }

  @Test @ExtendWith(PassthroughServer.class) @OffHeapResource(name = "limited-server-resource", size = 16)
  public void testDestroyFreesUpTheAllocatedResource(@Cluster URI clusterUri) throws CachePersistenceException {

    PersistentCacheManager persistentCacheManager = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource("limited-server-resource"))).build(true);

    CacheConfigurationBuilder<Long, String> configBuilder = newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
            .with(ClusteredResourcePoolBuilder.clusteredDedicated(8, MemoryUnit.MB)));

    persistentCacheManager.createCache("clusteredCache", configBuilder);
    try {
      persistentCacheManager.createCache("another-cache", configBuilder);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is("Cache 'another-cache' creation in EhcacheManager failed."));
    }

    persistentCacheManager.destroyCache("clusteredCache");

    Cache<Long, String> anotherCache = persistentCacheManager.createCache("another-cache", configBuilder);

    anotherCache.put(1L, "One");
    assertThat(anotherCache.get(1L), is("One"));

    persistentCacheManager.close();
  }

  @Test
  public void testDestroyUnknownCacheAlias(@Cluster URI clusterUri, @Cluster String resource) throws Exception {
    BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true).close();

    PersistentCacheManager cacheManager = newCacheManagerBuilder().with(cluster(clusterUri.resolve("/cache-manager")).expecting(c -> c.defaultServerResource(resource))).build(true);

    cacheManager.destroyCache(CLUSTERED_CACHE);

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> cacheManager.createCache(CLUSTERED_CACHE, newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder()
          .with(clustered()))));
    assertThat(e.getMessage(), containsString(CLUSTERED_CACHE));
    cacheManager.close();
  }

  @Test
  public void testDestroyNonExistentCache(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);

    String nonExistent = "this-is-not-the-cache-you-are-looking-for";
    assertThat(persistentCacheManager.getCache(nonExistent, Long.class, String.class), nullValue());
    persistentCacheManager.destroyCache(nonExistent);
    persistentCacheManager.close();
  }

  @Test
  public void testDestroyCacheWhenMultipleClientsConnected(@Cluster URI clusterUri, @Cluster String resource) {
    PersistentCacheManager persistentCacheManager1 = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);
    PersistentCacheManager persistentCacheManager2 = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);

    final Cache<Long, String> cache1 = persistentCacheManager1.getCache(CLUSTERED_CACHE, Long.class, String.class);

    final Cache<Long, String> cache2 = persistentCacheManager2.getCache(CLUSTERED_CACHE, Long.class, String.class);

    try {
      persistentCacheManager1.destroyCache(CLUSTERED_CACHE);
      fail();
    } catch (CachePersistenceException e) {
      assertThat(e.getMessage(), containsString("Cannot destroy cluster tier"));
    }

    try {
      cache1.put(1L, "One");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is("State is UNINITIALIZED"));
    }

    assertThat(cache2.get(1L), nullValue());

    cache2.put(1L, "One");

    assertThat(cache2.get(1L), is("One"));

    persistentCacheManager1.close();
    persistentCacheManager2.close();
  }

  private static Throwable getRootCause(Throwable t) {
    if (t.getCause() == null || t.getCause() == t) {
      return t;
    }
    return getRootCause(t.getCause());
  }

  @Test
  public void testDestroyCacheWithCacheManagerStopped(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);
    persistentCacheManager.close();
    persistentCacheManager.destroyCache(CLUSTERED_CACHE);
    assertThat(persistentCacheManager.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testDestroyNonExistentCacheWithCacheManagerStopped(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);
    persistentCacheManager.close();
    persistentCacheManager.destroyCache("this-is-not-the-cache-you-are-looking-for");
    assertThat(persistentCacheManager.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testDestroyCacheOnNonExistentCacheManager(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);
    persistentCacheManager.close();
    persistentCacheManager.destroy();

    persistentCacheManager.destroyCache("this-is-not-the-cache-you-are-looking-for");
    assertThat(persistentCacheManager.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testDestroyCacheWithTwoCacheManagerOnSameCache_forbiddenWhenInUse(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager1 = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);
    PersistentCacheManager persistentCacheManager2 = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);

    CachePersistenceException failure = assertThrows(CachePersistenceException.class, () -> persistentCacheManager1.destroyCache(CLUSTERED_CACHE));
    assertThat(failure.getMessage(), is("Cannot destroy cluster tier 'clustered-cache': in use by other client(s)"));
  }

  @Test
  public void testDestroyCacheWithTwoCacheManagerOnSameCache_firstRemovesSecondDestroy(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager1 = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);
    PersistentCacheManager persistentCacheManager2 = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);

    persistentCacheManager2.removeCache(CLUSTERED_CACHE);

    persistentCacheManager1.destroyCache(CLUSTERED_CACHE);
  }

  @Test
  public void testDestroyCacheWithTwoCacheManagerOnSameCache_secondDoesntHaveTheCacheButPreventExclusiveAccessToCluster(@Cluster URI clusterUri, @Cluster String resource) throws CachePersistenceException {
    PersistentCacheManager persistentCacheManager1 = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(false);
    PersistentCacheManager persistentCacheManager2 = BASE_BUILDER.with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource))).build(true);

    persistentCacheManager2.removeCache(CLUSTERED_CACHE);

    persistentCacheManager1.destroyCache(CLUSTERED_CACHE);
  }
}

