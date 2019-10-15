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
package org.ehcache.clustered.management;

import org.ehcache.CacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.testing.extension.TerracottaCluster;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredShared;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

@ExtendWith(TerracottaCluster.class)
@TerracottaCluster.OffHeapResource(name = "primary-server-resource", size = 64)
@TerracottaCluster.OffHeapResource(name = "secondary-server-resource", size = 64)
@TerracottaCluster.ClientLeaseLength(5)
public class EhcacheConfigWithManagementTest extends ClusteredTests {

  @Test
  public void create_cache_manager(@TerracottaCluster.Cluster URI clusterUri) throws Exception {
    CacheManager cacheManager = newCacheManagerBuilder()
      // cluster config
      .with(cluster(clusterUri.resolve("/my-server-entity-3"))
        .autoCreate(server -> server
          .defaultServerResource("primary-server-resource")
          .resourcePool("resource-pool-a", 10, MemoryUnit.MB, "secondary-server-resource") // <2>
          .resourcePool("resource-pool-b", 8, MemoryUnit.MB))) // will take from primary-server-resource
      // management config
      .using(new DefaultManagementRegistryConfiguration()
        .addTags("webapp-1", "server-node-1")
        .setCacheManagerAlias("my-super-cache-manager"))
      // cache config
      .withCache("dedicated-cache-1", newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(10, EntryUnit.ENTRIES)
          .offheap(1, MemoryUnit.MB)
          .with(clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB)))
        .build())
      .withCache("shared-cache-2", newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(10, EntryUnit.ENTRIES)
          .offheap(1, MemoryUnit.MB)
          .with(clusteredShared("resource-pool-a")))
        .build())
      .withCache("shared-cache-3", newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(10, EntryUnit.ENTRIES)
          .offheap(1, MemoryUnit.MB)
          .with(clusteredShared("resource-pool-b")))
        .build())
      .build(true);

    cacheManager.close();
  }

}
