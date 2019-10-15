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

package org.ehcache.clustered.replication;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Topology;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.units.MemoryUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.terracotta.passthrough.IClusterControl;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.LongStream;

import static java.util.Arrays.asList;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder.withConsistency;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The point of this test is to assert proper data read after fail-over handling.
 */
@WithSimpleTerracottaCluster @Topology(2)
@Execution(ExecutionMode.CONCURRENT)
public class BasicClusteredCacheOpsReplicationWithMultipleClientsTest extends ClusteredTests {

  @BeforeEach
  public void startServers(@Cluster IClusterControl clusterControl) throws Exception {
    clusterControl.startAllServers();
    clusterControl.waitForRunningPassivesInStandby();
  }

  private static CacheManager createCacheManager(URI clusterUri, String serverResource, String cacheName, Consistency consistency) {
    return newCacheManagerBuilder().with(cluster(clusterUri.resolve("/crud-cm-replication"))
      .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(20)))
      .autoCreate(server -> server.defaultServerResource(serverResource)))
      .withCache(cacheName,
        newCacheConfigurationBuilder(Long.class, BlobValue.class, heap(500).with(clusteredDedicated(4, MemoryUnit.MB)))
          .withService(withConsistency(consistency))).build(true);
  }

  @ParameterizedTest @Timeout(180)
  @EnumSource(Consistency.class)
  public void testCRUD(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;
    try (CacheManager manager1 = createCacheManager(clusterUri, serverResource, cacheName, consistency);
         CacheManager manager2 = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      Cache<Long, BlobValue> cache1 = manager1.getCache(cacheName, Long.class, BlobValue.class);
      Cache<Long, BlobValue> cache2 = manager2.getCache(cacheName, Long.class, BlobValue.class);

      Random random = new Random();
      LongStream longStream = random.longs(1000);
      Set<Long> added = new HashSet<>();
      longStream.forEach(x -> {
        cache1.put(x, new BlobValue());
        added.add(x);
      });

      Set<Long> readKeysByCache2BeforeFailOver = new HashSet<>();
      added.forEach(x -> {
        if (cache2.get(x) != null) {
          readKeysByCache2BeforeFailOver.add(x);
        }
      });

      clusterControl.terminateActive();

      Set<Long> readKeysByCache1AfterFailOver = new HashSet<>();
      added.forEach(x -> {
        if (cache1.get(x) != null) {
          readKeysByCache1AfterFailOver.add(x);
        }
      });

      assertThat(readKeysByCache2BeforeFailOver.size(), greaterThanOrEqualTo(readKeysByCache1AfterFailOver.size()));

      readKeysByCache1AfterFailOver.stream().filter(readKeysByCache2BeforeFailOver::contains).forEach(y -> assertThat(cache2.get(y), notNullValue()));
    }
  }

  @ParameterizedTest @Timeout(180)
  @EnumSource(Consistency.class)
  @Disabled("FIX THIS RANDOMLY FAILING TEST")
  public void testBulkOps(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;
    try (CacheManager manager1 = createCacheManager(clusterUri, serverResource, cacheName, consistency);
         CacheManager manager2 = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      Cache<Long, BlobValue> cache1 = manager1.getCache(cacheName, Long.class, BlobValue.class);
      Cache<Long, BlobValue> cache2 = manager2.getCache(cacheName, Long.class, BlobValue.class);

      List<Cache<Long, BlobValue>> caches = asList(cache1, cache2);

      Map<Long, BlobValue> entriesMap = new HashMap<>();

      Random random = new Random();
      LongStream longStream = random.longs(1000);

      longStream.forEach(x -> entriesMap.put(x, new BlobValue()));
      caches.forEach(cache -> cache.putAll(entriesMap));

      Set<Long> keySet = entriesMap.keySet();

      Set<Long> readKeysByCache2BeforeFailOver = new HashSet<>();
      keySet.forEach(x -> {
        if (cache2.get(x) != null) {
          readKeysByCache2BeforeFailOver.add(x);
        }
      });

      clusterControl.terminateActive();

      Set<Long> readKeysByCache1AfterFailOver = new HashSet<>();
      keySet.forEach(x -> {
        if (cache1.get(x) != null) {
          readKeysByCache1AfterFailOver.add(x);
        }
      });

      assertThat(readKeysByCache2BeforeFailOver.size(), greaterThanOrEqualTo(readKeysByCache1AfterFailOver.size()));

      readKeysByCache1AfterFailOver.stream().filter(readKeysByCache2BeforeFailOver::contains).forEach(y -> assertThat(cache2.get(y), notNullValue()));
    }
  }

  @ParameterizedTest @Timeout(180)
  @EnumSource(Consistency.class)
  public void testClear(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;
    try (CacheManager manager1 = createCacheManager(clusterUri, serverResource, cacheName, consistency);
         CacheManager manager2 = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      Cache<Long, BlobValue> cache1 = manager1.getCache(cacheName, Long.class, BlobValue.class);
      Cache<Long, BlobValue> cache2 = manager2.getCache(cacheName, Long.class, BlobValue.class);

      List<Cache<Long, BlobValue>> caches = new ArrayList<>();
      caches.add(cache1);
      caches.add(cache2);

      Map<Long, BlobValue> entriesMap = new HashMap<>();

      Random random = new Random();
      LongStream longStream = random.longs(1000);

      longStream.forEach(x -> entriesMap.put(x, new BlobValue()));
      caches.forEach(cache -> cache.putAll(entriesMap));

      Set<Long> keySet = entriesMap.keySet();

      Set<Long> readKeysByCache2BeforeFailOver = new HashSet<>();
      keySet.forEach(x -> {
        if (cache2.get(x) != null) {
          readKeysByCache2BeforeFailOver.add(x);
        }
      });

      cache1.clear();

      clusterControl.terminateActive();

      if (consistency == Consistency.STRONG) {
        readKeysByCache2BeforeFailOver.forEach(x -> assertThat(cache2.get(x), nullValue()));
      } else {
        readKeysByCache2BeforeFailOver.forEach(x -> assertThat(cache1.get(x), nullValue()));
      }
    }
  }

  private static class BlobValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] data = new byte[10 * 1024];
  }
}
