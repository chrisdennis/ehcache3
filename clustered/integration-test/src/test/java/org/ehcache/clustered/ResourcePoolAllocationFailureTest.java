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

import org.ehcache.CacheManager;
import org.ehcache.clustered.client.internal.PerpetualCachePersistenceException;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.Configuration;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManager;
import static org.ehcache.config.builders.ConfigurationBuilder.newConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

@WithSimpleTerracottaCluster
public class ResourcePoolAllocationFailureTest extends ClusteredTests {

  @Test
  public void testTooLowResourceException(@Cluster URI clusterUri, @Cluster String serverResource) {

    Configuration illegal = newConfigurationBuilder()
      .withService(cluster(clusterUri.resolve("/crud-cm")).autoCreate(server -> server.defaultServerResource(serverResource)))
      .withCache("test-cache", newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder().with(clusteredDedicated(10, MemoryUnit.KB)))
        .withService(new ClusteredStoreConfiguration(Consistency.EVENTUAL)))
      .build();

    Exception e = assertThrows(Exception.class, () -> newCacheManager(illegal).init());
    Throwable cause = getCause(e, PerpetualCachePersistenceException.class);
    assertThat(cause, notNullValue());
    assertThat(cause.getMessage(), startsWith("Unable to create"));

    Configuration legal = illegal.derive().updateCache("test-cache",
      builder -> builder.updateResourcePools(
        old -> newResourcePoolsBuilder().with(clusteredDedicated(100, MemoryUnit.KB)).build())).build();

    try (CacheManager cacheManager = newCacheManager(legal)) {
      cacheManager.init();
      assertThat(cacheManager, notNullValue());
    }
  }

  private static Throwable getCause(Throwable e, Class<? extends Throwable> causeClass) {
    Throwable current = e;
    while (current.getCause() != null) {
      if (current.getClass().isAssignableFrom(causeClass)) {
        return current;
      }
      current = current.getCause();
    }
    return null;
  }
}
