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
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test asserts Active-Passive fail-over with
 * multi-threaded/multi-client scenarios.
 * Note that fail-over is happening while client threads are still writing
 * Finally the same key set correctness is asserted.
 */
@WithSimpleTerracottaCluster @Topology(2)
@Execution(ExecutionMode.CONCURRENT)
public class BasicClusteredCacheOpsReplicationMultiThreadedTest extends ClusteredTests {

  private static final int NUM_OF_THREADS = 10;
  private static final int JOB_SIZE = 100;

  private final ThreadLocalRandom random = ThreadLocalRandom.current();

  @BeforeEach
  public void startServers(@Cluster IClusterControl clusterControl) throws Exception {
    clusterControl.startAllServers();
    clusterControl.waitForActive();
    clusterControl.waitForRunningPassivesInStandby();
  }

  private static CacheManager createCacheManager(URI clusterUri, String serverResource, String cacheName, Consistency consistency) {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/crud-cm-replication"))
            .timeouts(TimeoutsBuilder.timeouts() // we need to give some time for the failover to occur
                .read(Duration.ofMinutes(1))
                .write(Duration.ofMinutes(1)))
            .autoCreate(server -> server.defaultServerResource(serverResource)))
      .withCache(cacheName, newCacheConfigurationBuilder(Long.class, BlobValue.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(500, EntryUnit.ENTRIES)
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB)))
        .withService(ClusteredStoreConfigurationBuilder.withConsistency(consistency)));
    return clusteredCacheManagerBuilder.build(true);
  }

  @ParameterizedTest @Timeout(180)
  @EnumSource(Consistency.class)
  public void testCRUD(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;
    try (CacheManager manager1 = createCacheManager(clusterUri, serverResource, cacheName, consistency);
         CacheManager manager2 = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      Cache<Long, BlobValue> cache1 = manager1.getCache(cacheName, Long.class, BlobValue.class);
      Cache<Long, BlobValue> cache2 = manager2.getCache(cacheName, Long.class, BlobValue.class);
      List<Cache<Long, BlobValue>> caches = asList(cache1, cache2);

      ExecutorService executorService = Executors.newWorkStealingPool(NUM_OF_THREADS);
      try {
        Set<Long> universalSet = ConcurrentHashMap.newKeySet();
        List<Future<?>> futures = new ArrayList<>();

        caches.forEach(cache -> {
          for (int i = 0; i < NUM_OF_THREADS; i++) {
            futures.add(executorService.submit(() -> random.longs().limit(JOB_SIZE).forEach(x -> {
              cache.put(x, new BlobValue());
              universalSet.add(x);
            })));
          }
        });

        //This step is to add values in local tier randomly to test invalidations happen correctly
        futures.add(executorService.submit(() -> universalSet.forEach(x -> {
          cache1.get(x);
          cache2.get(x);
        })));

        clusterControl.terminateActive();

        drainTasks(futures);

        Set<Long> readKeysByCache1AfterFailOver = new HashSet<>();
        Set<Long> readKeysByCache2AfterFailOver = new HashSet<>();
        universalSet.forEach(x -> {
          if (cache1.get(x) != null) {
            readKeysByCache1AfterFailOver.add(x);
          }
          if (cache2.get(x) != null) {
            readKeysByCache2AfterFailOver.add(x);
          }
        });

        assertThat(readKeysByCache2AfterFailOver.size(), equalTo(readKeysByCache1AfterFailOver.size()));

        readKeysByCache2AfterFailOver.stream().forEach(y -> assertThat(readKeysByCache1AfterFailOver.contains(y), is(true)));
      } finally {
        assertThat(executorService.shutdownNow(), is(empty()));
      }
    }
  }

  @ParameterizedTest @Timeout(180)
  @EnumSource(Consistency.class)
  public void testBulkOps(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;
    try (CacheManager manager1 = createCacheManager(clusterUri, serverResource, cacheName, consistency);
         CacheManager manager2 = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      Cache<Long, BlobValue> cache1 = manager1.getCache(cacheName, Long.class, BlobValue.class);
      Cache<Long, BlobValue> cache2 = manager2.getCache(cacheName, Long.class, BlobValue.class);
      List<Cache<Long, BlobValue>> caches = asList(cache1, cache2);

      ExecutorService executorService = Executors.newWorkStealingPool(NUM_OF_THREADS);
      try {
        Set<Long> universalSet = ConcurrentHashMap.newKeySet();
        List<Future<?>> futures = new ArrayList<>();

        caches.forEach(cache -> {
          for (int i = 0; i < NUM_OF_THREADS; i++) {
            Map<Long, BlobValue> map = random.longs().limit(JOB_SIZE).collect(HashMap::new, (hashMap, x) -> hashMap.put(x, new BlobValue()), HashMap::putAll);
            futures.add(executorService.submit(() -> {
              cache.putAll(map);
              universalSet.addAll(map.keySet());
            }));
          }
        });

        //This step is to add values in local tier randomly to test invalidations happen correctly
        futures.add(executorService.submit(() -> {
          universalSet.forEach(x -> {
            cache1.get(x);
            cache2.get(x);
          });
        }));

        clusterControl.terminateActive();

        drainTasks(futures);

        Set<Long> readKeysByCache1AfterFailOver = new HashSet<>();
        Set<Long> readKeysByCache2AfterFailOver = new HashSet<>();
        universalSet.forEach(x -> {
          if (cache1.get(x) != null) {
            readKeysByCache1AfterFailOver.add(x);
          }
          if (cache2.get(x) != null) {
            readKeysByCache2AfterFailOver.add(x);
          }
        });

        assertThat(readKeysByCache2AfterFailOver.size(), equalTo(readKeysByCache1AfterFailOver.size()));

        readKeysByCache2AfterFailOver.stream().forEach(y -> assertThat(readKeysByCache1AfterFailOver.contains(y), is(true)));
      } finally {
        assertThat(executorService.shutdownNow(), is(empty()));
      }
    }
  }

  @Disabled("This is currently unstable as if the clear does not complete before the failover," +
          "there is no future operation that will trigger the code in ClusterTierActiveEntity.invokeServerStoreOperation" +
          "dealing with in-flight invalidation reconstructed from reconnect data")
  @ParameterizedTest @Timeout(180)
  @EnumSource(Consistency.class)
  public void testClear(Consistency consistency, @Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new) + "-" + consistency;
    try (CacheManager manager1 = createCacheManager(clusterUri, serverResource, cacheName, consistency);
         CacheManager manager2 = createCacheManager(clusterUri, serverResource, cacheName, consistency)) {
      Cache<Long, BlobValue> cache1 = manager1.getCache(cacheName, Long.class, BlobValue.class);
      Cache<Long, BlobValue> cache2 = manager2.getCache(cacheName, Long.class, BlobValue.class);
      List<Cache<Long, BlobValue>> caches = asList(cache1, cache2);

      ExecutorService executorService = Executors.newWorkStealingPool(NUM_OF_THREADS);
      try {
        List<Future<?>> futures = new ArrayList<>();
        Set<Long> universalSet = ConcurrentHashMap.newKeySet();

        caches.forEach(cache -> {
          for (int i = 0; i < NUM_OF_THREADS; i++) {
            Map<Long, BlobValue> map = random.longs().limit(JOB_SIZE).collect(HashMap::new, (hashMap, x) -> hashMap.put(x, new BlobValue()), HashMap::putAll);
            futures.add(executorService.submit(() -> {
              cache.putAll(map);
              universalSet.addAll(map.keySet());
            }));
          }
        });

        drainTasks(futures);

        universalSet.forEach(x -> {
          cache1.get(x);
          cache2.get(x);
        });

        Future<?> clearFuture = executorService.submit(() -> cache1.clear());

        clusterControl.terminateActive();

        clearFuture.get();

        universalSet.forEach(x -> assertThat(cache2.get(x), nullValue()));
      } finally {
        assertThat(executorService.shutdownNow(), is(empty()));
      }
    }
  }

  private void drainTasks(List<Future<?>> futures) throws InterruptedException, java.util.concurrent.ExecutionException {
    for (int i = 0; i < futures.size(); i++) {
      try {
        futures.get(i).get(60, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        fail("Stuck on number " + i);
      }
    }
  }

  private static class BlobValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] data = new byte[10 * 1024];
  }

}
