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
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.Test;

import com.tc.util.Assert;
import org.terracotta.passthrough.IClusterControl;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simulate multiple clients starting up the same cache manager simultaneously and ensure that puts and gets works just
 * fine and nothing get lost or hung, just because multiple cache manager instances of the same cache manager are coming up
 * simultaneously.
 */
@WithSimpleTerracottaCluster
public class BasicCacheOpsMultiThreadedTest extends ClusteredTests {

  private static final String CLUSTERED_CACHE_NAME    = "clustered-cache";
  private static final String SYN_CACHE_NAME = "syn-cache";
  private static final String CACHE_MANAGER_NAME = "/crud-cm";
  private static final int PRIMARY_SERVER_RESOURCE_SIZE = 4; //MB
  private static final int NUM_THREADS = 8;
  private static final int MAX_WAIT_TIME_SECONDS = 30;

  private final AtomicReference<Throwable> exception = new AtomicReference<>();
  private final AtomicLong idGenerator = new AtomicLong(2L);

  @Test
  public void testMultipleClients(@Cluster URI clusterUri, @Cluster String serverResource) throws Throwable {
    CountDownLatch latch = new CountDownLatch(NUM_THREADS + 1);

    List<Thread> threads = new ArrayList<>(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      Thread t1 = new Thread(content(clusterUri, serverResource, latch));
      t1.start();
      threads.add(t1);
    }

    latch.countDown();
    assertTrue(latch.await(MAX_WAIT_TIME_SECONDS, TimeUnit.SECONDS));

    for (Thread t : threads) {
      t.join();
    }

    Throwable throwable = exception.get();
    if (throwable != null) {
      throw throwable;
    }
  }

  private Runnable content(URI clusterUri, String serverResource, CountDownLatch latch) {
    return () -> {
      try (PersistentCacheManager cacheManager = createCacheManager(clusterUri, serverResource)) {
        latch.countDown();
        assertTrue(latch.await(MAX_WAIT_TIME_SECONDS, TimeUnit.SECONDS));

        cacheManager.init();
        doSyncAndPut(cacheManager);
      } catch (Throwable t) {
        if (!exception.compareAndSet(null, t)) {
          exception.get().addSuppressed(t);
        }
      }
    };
  }

  private void doSyncAndPut(PersistentCacheManager cacheManager) throws InterruptedException {
    String customValue = "value";
    Cache<String, Boolean> synCache = cacheManager.getCache(SYN_CACHE_NAME, String.class, Boolean.class);
    Cache<Long, String> customValueCache = cacheManager.getCache(CLUSTERED_CACHE_NAME, Long.class, String.class);
    parallelPuts(customValueCache);
    String firstClientStartKey = "first_client_start", firstClientEndKey = "first_client_end";
    if (synCache.putIfAbsent(firstClientStartKey, true) == null) {
      customValueCache.put(1L, customValue);
      assertThat(customValueCache.get(1L), is(customValue));
      synCache.put(firstClientEndKey, true);
    } else {
      int retry = 0, maxRetryCount = 30;
      while (++retry <= maxRetryCount && synCache.get(firstClientEndKey) == null) {
        Thread.sleep(1000L);
      }

      if (retry > maxRetryCount) {
        Assert.fail("Couldn't find " + firstClientEndKey + " in synCache after " + maxRetryCount + " retries!");
      }

      assertThat(customValueCache.get(1L), is(customValue));
    }
  }

  private static PersistentCacheManager createCacheManager(URI clusterURI, String serverResource) {
    ClusteringServiceConfigurationBuilder clusteringConfig = cluster(clusterURI.resolve(CACHE_MANAGER_NAME))
      .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
      .autoCreate(server -> server.defaultServerResource(serverResource));

    ResourcePool resourcePool = ClusteredResourcePoolBuilder
      .clusteredDedicated(PRIMARY_SERVER_RESOURCE_SIZE, MemoryUnit.MB);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder = CacheManagerBuilder
      .newCacheManagerBuilder()
      .with(clusteringConfig)
      .withCache(CLUSTERED_CACHE_NAME,
        CacheConfigurationBuilder
          .newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
              .with(resourcePool))
          .withService(new ClusteredStoreConfiguration(Consistency.STRONG)))
      .withCache(SYN_CACHE_NAME,
        CacheConfigurationBuilder
          .newCacheConfigurationBuilder(String.class, Boolean.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
              .with(resourcePool))
          .withService(new ClusteredStoreConfiguration(Consistency.STRONG)));
    return clusteredCacheManagerBuilder.build(false);
  }

  private void parallelPuts(Cache<Long, String> customValueCache) {
    // make sure each thread gets its own id
    long startingId = idGenerator.getAndAdd(10L);
    customValueCache.put(startingId + 1, "value1");
    customValueCache.put(startingId + 1, "value11");
    customValueCache.put(startingId + 2, "value2");
    customValueCache.put(startingId + 3, "value3");
    customValueCache.put(startingId + 4, "value4");
    assertThat(customValueCache.get(startingId + 1), is("value11"));
    assertThat(customValueCache.get(startingId + 2), is("value2"));
    assertThat(customValueCache.get(startingId + 3), is("value3"));
    assertThat(customValueCache.get(startingId + 4), is("value4"));
  }
}
