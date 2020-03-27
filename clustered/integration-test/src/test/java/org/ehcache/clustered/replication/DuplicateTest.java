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
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.StoreAccessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.terracotta.passthrough.IClusterControl;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@WithSimpleTerracottaCluster @Topology(2)
public class DuplicateTest extends ClusteredTests {

  private PersistentCacheManager cacheManager;

  @BeforeEach
  public void waitForPassive(@Cluster IClusterControl clusterControl) throws Exception {
    clusterControl.startAllServers();
    clusterControl.waitForRunningPassivesInStandby();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if(cacheManager != null) {
      cacheManager.close();
      cacheManager.destroy();
    }
  }

  @Test
  public void duplicateAfterFailoverAreReturningTheCorrectResponse(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri)
        .timeouts(TimeoutsBuilder.timeouts().write(Duration.ofSeconds(30)))
        .autoCreate(server -> server.defaultServerResource(serverResource)))
      .withCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(10, MemoryUnit.MB)))
        .withResilienceStrategy(failingResilienceStrategy())
        .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG)));

    cacheManager =  builder.build(true);

    Cache<Integer, String> cache = cacheManager.getCache("cache", Integer.class, String.class);

    int numEntries = 3000;
    AtomicInteger currentEntry = new AtomicInteger();

    //Perform put operations in another thread
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      Future<?> puts = executorService.submit(() -> {
        while (true) {
          int i = currentEntry.getAndIncrement();
          if (i >= numEntries) {
            break;
          }
          cache.put(i, "value:" + i);
        }
      });

      while (currentEntry.get() < 100); // wait to make sure some entries are added before shutdown

      // Failover to mirror when put & replication are in progress
      clusterControl.terminateActive();

      puts.get(30, TimeUnit.SECONDS);

      //Verify cache entries on mirror
      for (int i = 0; i < numEntries; i++) {
        assertThat(cache.get(i)).isEqualTo("value:" + i);
      }
    } finally {
      executorService.shutdownNow();
    }

  }

  @SuppressWarnings("unchecked")
  private ResilienceStrategy<Integer, String> failingResilienceStrategy() throws Exception {
    return (ResilienceStrategy<Integer, String>)
      Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] { ResilienceStrategy.class},
        (proxy, method, args) -> {
          if(method.getName().endsWith("Failure")) {
            fail("Failure on " + method.getName(), findStoreAccessException(args)); // one param is always a SAE
            return null;
          }

          switch(method.getName()) {
            case "hashCode":
              return 0;
            case "equals":
              return proxy == args[0];
            default:
              fail("Unexpected method call: " + method.getName());
              return null;
          }
        });
  }

  private StoreAccessException findStoreAccessException(Object[] objects) {
    for(Object o : objects) {
      if (o instanceof StoreAccessException) {
        return (StoreAccessException) o;
      }
    }
    fail("There should be an exception somewhere in " + Arrays.toString(objects));
    return null;
  }
}
