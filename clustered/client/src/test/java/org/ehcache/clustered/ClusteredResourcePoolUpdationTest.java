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
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(PassthroughServer.class)
@PassthroughServer.OffHeapResource(name = "primary-server-resource", size = 8)
@PassthroughServer.OffHeapResource(name = "secondary-server-resource", size = 8)
public class ClusteredResourcePoolUpdationTest {

  private static PersistentCacheManager cacheManager;
  private static Cache<Long, String> dedicatedCache;
  private static Cache<Long, String> sharedCache;

  @BeforeAll
  public static void setUp(@Cluster URI clusterUri) {
    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/cache-manager")).autoCreate(server -> server
        .defaultServerResource("primary-server-resource")
        .resourcePool("resource-pool-a", 2, MemoryUnit.MB, "secondary-server-resource")
        .resourcePool("resource-pool-b", 4, MemoryUnit.MB)))
      .withCache("dedicated-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB))))
      .withCache("shared-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool-a"))))
      .build();
    cacheManager.init();

    dedicatedCache = cacheManager.getCache("dedicated-cache", Long.class, String.class);
    sharedCache = cacheManager.getCache("shared-cache", Long.class, String.class);
  }

  @AfterAll
  public static void tearDown() {
    cacheManager.close();
  }

  @Test
  public void testClusteredDedicatedResourcePoolUpdation() {
    UnsupportedOperationException failure = assertThrows(UnsupportedOperationException.class, () -> dedicatedCache.getRuntimeConfiguration().updateResourcePools(
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB))
        .build()
    ));
    assertThat(failure.getMessage(), is("Updating CLUSTERED resource is not supported"));
  }

  @Test
  public void testClusteredSharedResourcePoolUpdation() {
    UnsupportedOperationException failure = assertThrows(UnsupportedOperationException.class, () -> sharedCache.getRuntimeConfiguration().updateResourcePools(
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool-a"))
        .build()
    ));
    assertThat(failure.getMessage(), is("Updating CLUSTERED resource is not supported"));
  }
}
