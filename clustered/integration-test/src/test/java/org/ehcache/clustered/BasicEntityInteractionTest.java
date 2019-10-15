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

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Properties;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.common.EhcacheEntityVersion;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.management.cluster.DefaultClusteringManagementService;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;

import static java.util.Collections.emptyMap;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.terracotta.connection.ConnectionFactory.connect;

@WithSimpleTerracottaCluster
@Execution(ExecutionMode.CONCURRENT)
public class BasicEntityInteractionTest extends ClusteredTests {

  private ClusterTierManagerConfiguration blankConfiguration = new ClusterTierManagerConfiguration("identifier", new ServerSideConfiguration(emptyMap()));

  private TestInfo testInfo;

  @BeforeEach
  public void getTestName(TestInfo testInfo) {
    this.testInfo = testInfo;
  }

  @Test
  public void testClusteringServiceConfigurationBuilderThrowsNPE(@Cluster URI tsaUri, @Cluster String serverResource) {
    String cacheName = "myCACHE";

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache(cacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(100, ENTRIES)
        .with(clusteredDedicated(2, MemoryUnit.MB)))
      ).with(ClusteringServiceConfigurationBuilder.cluster(tsaUri)
        .autoCreate(server -> server.defaultServerResource(serverResource))
      ).build(true)) {
      Cache<Long, String> cache = cacheManager.getCache(cacheName, Long.class, String.class);
      cache.put(1L, "one");
    }

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache(cacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
          .heap(100, ENTRIES)
          .with(clusteredDedicated(2, MemoryUnit.MB))
        )
      ).with(ClusteringServiceConfigurationBuilder.cluster(tsaUri)
      ).using(new DefaultStatisticsService()
      ).using(new DefaultClusteringManagementService()
      ).build(true)) {
      Cache<Long, String> cache = cacheManager.getCache(cacheName, Long.class, String.class);
      cache.get(1L);
    }

  }

  @Test
  public void testServicesStoppedTwice(@Cluster URI tsaUri, @Cluster String serverResource) {
    String cacheName = "myCACHE";

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache(cacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(100, ENTRIES)
        .with(clusteredDedicated(2, MemoryUnit.MB)))
      ).with(ClusteringServiceConfigurationBuilder.cluster(tsaUri)
        .autoCreate(server -> server.defaultServerResource(serverResource))
        // manually adding the following two services should work
      ).using(new DefaultStatisticsService()
      ).using(new DefaultClusteringManagementService()
      ).build(true)) {
      Cache<Long, String> cache = cacheManager.getCache(cacheName, Long.class, String.class);
      cache.put(1L, "one");
    }

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache(cacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
          .heap(100, ENTRIES)
          .with(clusteredDedicated(2, MemoryUnit.MB))
        )
      ).with(ClusteringServiceConfigurationBuilder.cluster(tsaUri)
      ).build(true)) {
      Cache<Long, String> cache = cacheManager.getCache(cacheName, Long.class, String.class);
      cache.get(1L);
    }

  }

  @Test
  public void testAbsentEntityRetrievalFails(@Cluster URI clusterUri) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      try {
        ref.fetchEntity(null);
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    }
  }

  @Test
  public void testAbsentEntityCreationSucceeds(@Cluster URI clusterUri) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      assertThat(ref.fetchEntity(null), not(Matchers.nullValue()));
    }
  }

  @Test
  public void testPresentEntityCreationFails(@Cluster URI clusterUri) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      try {
        try {
          ref.create(blankConfiguration);
          fail("Expected EntityAlreadyExistsException");
        } catch (EntityAlreadyExistsException e) {
          //expected
        }

        ClusterTierManagerConfiguration otherConfiguration = new ClusterTierManagerConfiguration("different", new ServerSideConfiguration(emptyMap()));
        try {
          ref.create(otherConfiguration);
          fail("Expected EntityAlreadyExistsException");
        } catch (EntityAlreadyExistsException e) {
          //expected
        }
      } finally {
        ref.destroy();
      }
    }
  }

  @Test
  public void testAbsentEntityDestroyFails(@Cluster URI clusterUri) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      try {
        ref.destroy();
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    }
  }

  @Test
  public void testPresentEntityDestroySucceeds(@Cluster URI clusterUri) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      ref.destroy();

      try {
        ref.fetchEntity(null);
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    }
  }

  @Test
  @Disabled
  @SuppressWarnings("try")
  public void testPresentEntityDestroyBlockedByHeldReferenceSucceeds(@Cluster URI clusterUri) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);

      try (ClusterTierManagerClientEntity entity = ref.fetchEntity(null)) {
        ref.destroy();
      }
    }
  }

  @Test
  public void testPresentEntityDestroyNotBlockedByReleasedReferenceSucceeds(@Cluster URI clusterUri) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      ref.fetchEntity(null).close();
      ref.destroy();
    }
  }

  @Test
  public void testDestroyedEntityAllowsRecreation(@Cluster URI clusterUri) throws Throwable {
    try (Connection client = connect(clusterUri, new Properties())) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      ref.destroy();

      ref.create(blankConfiguration);
      assertThat(ref.fetchEntity(null), not(nullValue()));
    }
  }

  private EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> getEntityRef(Connection client) throws org.terracotta.exception.EntityNotProvidedException {
    return client.getEntityRef(ClusterTierManagerClientEntity.class, EhcacheEntityVersion.ENTITY_VERSION, testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new));
  }
}
