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

import com.tc.net.proxy.TCPProxy;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.testing.extension.TerracottaCluster.ClientLeaseLength;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@WithSimpleTerracottaCluster
@ClientLeaseLength(5)
public class LeaseTest extends ClusteredTests {

  public static Stream<Arguments> data() {
    return Stream.of(
      arguments(newResourcePoolsBuilder().with(clusteredDedicated(1, MemoryUnit.MB))),
      arguments(newResourcePoolsBuilder().with(clusteredDedicated(1, MemoryUnit.MB)).heap(10, EntryUnit.ENTRIES))
    );
  }

  @ParameterizedTest @MethodSource("data")
  public void leaseExpiry(ResourcePoolsBuilder resources, @Cluster URI clusterUri, @Cluster String serverResource) throws Exception {
    List<TCPProxy> proxies = new ArrayList<>();
    try {
      URI connectionURI = TCPProxyUtil.getProxyURI(clusterUri, proxies);

      CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder = newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
          .timeouts(TimeoutsBuilder.timeouts().connection(Duration.ofSeconds(20)))
          .autoCreate(server -> server.defaultServerResource(serverResource)));
      PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
      cacheManager.init();

      CacheConfiguration<Long, String> config = newCacheConfigurationBuilder(Long.class, String.class, resources).build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
      cache.put(1L, "The one");
      cache.put(2L, "The two");
      cache.put(3L, "The three");
      assertThat(cache.get(1L), equalTo("The one"));
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));

      setDelay(6000, proxies);
      Thread.sleep(6000);
      // We will now have lost the lease

      setDelay(0L, proxies);

      AtomicBoolean timedout = new AtomicBoolean(false);

      CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
        while (!timedout.get()) {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            throw new AssertionError(e);
          }
          String result = cache.get(1L);
          if (result != null) {
            return result;
          }
        }
        return null;
      });

      assertThat(future.get(30, TimeUnit.SECONDS), is("The one"));

      timedout.set(true);

      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));
    } finally {
      proxies.forEach(TCPProxy::stop);
    }
  }
}
