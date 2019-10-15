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
package org.ehcache.clustered.reconnect;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.Test;
import org.terracotta.passthrough.IClusterControl;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@WithSimpleTerracottaCluster
public class AutoCreateOnReconnectTest extends ClusteredTests {

  @Test
  public void cacheManagerCanReconnect(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource) throws Exception {
    try (PersistentCacheManager cacheManager = newCacheManagerBuilder()
      .with(cluster(clusterUri.resolve("/crud-cm"))
        .autoCreateOnReconnect(server -> server.defaultServerResource(serverResource)))
      .build(true)) {

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder()
          .with(clusteredDedicated(1, MemoryUnit.MB)))
          .build());

      cache.put(1L, "one");

      clusterControl.terminateAllServers();
      clusterControl.startAllServers();

      while (cache.get(1L) == null) {
        Thread.sleep(100);
        cache.put(1L, "two");
      }
      assertThat(cache.get(1L), is("two"));
    }
  }
}
