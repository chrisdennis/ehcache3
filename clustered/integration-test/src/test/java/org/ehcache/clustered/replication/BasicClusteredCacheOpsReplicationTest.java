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
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Topology;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.terracotta.passthrough.IClusterControl;

import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@WithSimpleTerracottaCluster @Topology(2)
@Execution(ExecutionMode.CONCURRENT)
public class BasicClusteredCacheOpsReplicationTest extends ClusteredTests {

  @BeforeEach
  public void startServers(@Cluster IClusterControl clusterControl) throws Exception {
    clusterControl.startAllServers();
    clusterControl.waitForRunningPassivesInStandby();
  }

  private static CacheManager createCacheManager(@Cluster URI clusterUri, @Cluster String serverResource, String cacheName, Consistency cacheConsistency) {
    CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES)
        .with(ClusteredResourcePoolBuilder.clusteredDedicated(1, MemoryUnit.MB)))
      .withService(ClusteredStoreConfigurationBuilder.withConsistency(cacheConsistency))
      .build();

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/cm-replication"))
            .timeouts(TimeoutsBuilder.timeouts() // we need to give some time for the failover to occur
                .read(Duration.ofMinutes(1))
                .write(Duration.ofMinutes(1)))
            .autoCreate(server -> server.defaultServerResource(serverResource)))
      .withCache(cacheName + "-1", config)
      .withCache(cacheName + "-2", config);

    return clusteredCacheManagerBuilder.build(true);
  }

  @ParameterizedTest @EnumSource(Consistency.class)
  public void testCRUD(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;

    try (CacheManager cacheManager = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cacheManager.getCache(cacheName + "-1", Long.class, String.class));
      caches.add(cacheManager.getCache(cacheName + "-2", Long.class, String.class));
      caches.forEach(x -> {
        x.put(1L, "The one");
        x.put(2L, "The two");
        x.put(1L, "Another one");
        x.put(3L, "The three");
        x.put(4L, "The four");
        assertThat(x.get(1L), equalTo("Another one"));
        assertThat(x.get(2L), equalTo("The two"));
        assertThat(x.get(3L), equalTo("The three"));
        x.remove(4L);
      });

      clusterControl.terminateActive();

      caches.forEach(x -> {
        assertThat(x.get(1L), equalTo("Another one"));
        assertThat(x.get(2L), equalTo("The two"));
        assertThat(x.get(3L), equalTo("The three"));
        assertThat(x.get(4L), nullValue());
      });
    }
  }

  @ParameterizedTest @EnumSource(Consistency.class)
  public void testBulkOps(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;

    try (CacheManager cacheManager = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cacheManager.getCache(cacheName + "-1", Long.class, String.class));
      caches.add(cacheManager.getCache(cacheName + "-2", Long.class, String.class));

      Map<Long, String> entriesMap = new HashMap<>();
      entriesMap.put(1L, "one");
      entriesMap.put(2L, "two");
      entriesMap.put(3L, "three");
      entriesMap.put(4L, "four");
      entriesMap.put(5L, "five");
      entriesMap.put(6L, "six");
      caches.forEach(cache -> cache.putAll(entriesMap));

      clusterControl.terminateActive();

      Set<Long> keySet = entriesMap.keySet();
      caches.forEach(cache -> {
        Map<Long, String> all = cache.getAll(keySet);
        assertThat(all.get(1L), is("one"));
        assertThat(all.get(2L), is("two"));
        assertThat(all.get(3L), is("three"));
        assertThat(all.get(4L), is("four"));
        assertThat(all.get(5L), is("five"));
        assertThat(all.get(6L), is("six"));
      });
    }
  }

  @ParameterizedTest @EnumSource(Consistency.class)
  public void testCAS(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;

    try (CacheManager cacheManager = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cacheManager.getCache(cacheName + "-1", Long.class, String.class));
      caches.add(cacheManager.getCache(cacheName + "-2", Long.class, String.class));
      caches.forEach(cache -> {
        assertThat(cache.putIfAbsent(1L, "one"), nullValue());
        assertThat(cache.putIfAbsent(2L, "two"), nullValue());
        assertThat(cache.putIfAbsent(3L, "three"), nullValue());
        assertThat(cache.replace(3L, "another one", "yet another one"), is(false));
      });

      clusterControl.terminateActive();

      caches.forEach(cache -> {
        assertThat(cache.putIfAbsent(1L, "another one"), is("one"));
        assertThat(cache.remove(2L, "not two"), is(false));
        assertThat(cache.replace(3L, "three", "another three"), is(true));
        assertThat(cache.replace(2L, "new two"), is("two"));
      });
    }
  }

  @ParameterizedTest @EnumSource(Consistency.class)
  public void testClear(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;

    try (CacheManager cacheManager = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cacheManager.getCache(cacheName + "-1", Long.class, String.class));
      caches.add(cacheManager.getCache(cacheName + "-2", Long.class, String.class));

      Map<Long, String> entriesMap = new HashMap<>();
      entriesMap.put(1L, "one");
      entriesMap.put(2L, "two");
      entriesMap.put(3L, "three");
      entriesMap.put(4L, "four");
      entriesMap.put(5L, "five");
      entriesMap.put(6L, "six");
      caches.forEach(cache -> cache.putAll(entriesMap));

      Set<Long> keySet = entriesMap.keySet();
      caches.forEach(cache -> {
        Map<Long, String> all = cache.getAll(keySet);
        assertThat(all.get(1L), is("one"));
        assertThat(all.get(2L), is("two"));
        assertThat(all.get(3L), is("three"));
        assertThat(all.get(4L), is("four"));
        assertThat(all.get(5L), is("five"));
        assertThat(all.get(6L), is("six"));
      });

      caches.forEach(Cache::clear);

      clusterControl.terminateActive();

      caches.forEach(cache -> {
        keySet.forEach(x -> assertThat(cache.get(x), nullValue()));
      });
    }
  }
}
