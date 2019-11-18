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

package org.ehcache.clustered.client.docs;

import java.net.URI;

import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.client.internal.PassthroughServer.OffHeapResource;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;

/**
 * Tiering
 */
@ExtendWith(PassthroughServer.class)
@OffHeapResource(name = "primary-server-resource", size = 64)
public class Tiering {

  @Test
  public void testSingleTier() {
    // tag::clusteredOnly[]
    CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <1>
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredDedicated(2, MemoryUnit.GB))); // <2>
    // end::clusteredOnly[]
  }

  @Test
  public void threeTiersCacheManager(@Cluster URI clusterUri) {
    // tag::threeTiersCacheManager[]
    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c)) // <1>
      .withCache("threeTierCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES) // <2>
            .offheap(1, MemoryUnit.MB) // <3>
            .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)) // <4>
        )
      ).build(true);
    // end::threeTiersCacheManager[]

    persistentCacheManager.close();
  }
}
