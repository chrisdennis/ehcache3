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
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.clustered.util.TestCacheLoaderWriter;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.resilience.ThrowingResilienceStrategy;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

@WithSimpleTerracottaCluster
@Execution(ExecutionMode.CONCURRENT)
public class ClusteredLoaderWriterTest extends ClusteredTests {

  @ParameterizedTest
  @EnumSource(Consistency.class)
  public void testBasicOps(Consistency cacheConsistency, @Cluster URI clusterUri, @Cluster String serverResource) {
    ConcurrentMap<Long, String> sor = new ConcurrentHashMap<>();
    CacheConfiguration<Long, String> configuration = getCacheConfig(cacheConsistency, sor);

    Cache<Long, String> client1 = newCacheManager(clusterUri, serverResource).createCache("basicops" + cacheConsistency.name(), configuration);
    assertThat(sor.isEmpty(), is(true));

    Set<Long> keys = new HashSet<>();
    ThreadLocalRandom.current().longs(10).forEach(x -> {
      keys.add(x);
      client1.put(x, Long.toString(x));
    });

    assertThat(sor.size(), is(10));

    CacheManager anotherCacheManager = newCacheManager(clusterUri, serverResource);
    Cache<Long, String> client2 = anotherCacheManager.createCache("basicops" + cacheConsistency.name(),
            getCacheConfig(cacheConsistency, sor));
    Map<Long, String> all = client2.getAll(keys);
    assertThat(all.keySet(), containsInAnyOrder(keys.toArray()));

    keys.stream().limit(3).forEach(client2::remove);

    assertThat(sor.size(), is(7));
  }

  @ParameterizedTest
  @EnumSource(Consistency.class)
  public void testCASOps(Consistency cacheConsistency, @Cluster URI clusterUri, @Cluster String serverResource) {
    ConcurrentMap<Long, String> sor = new ConcurrentHashMap<>();
    CacheConfiguration<Long, String> configuration = getCacheConfig(cacheConsistency, sor);

    Cache<Long, String> client1 = newCacheManager(clusterUri, serverResource).createCache("casops" + cacheConsistency.name(), configuration);
    assertThat(sor.isEmpty(), is(true));

    Set<Long> keys = new HashSet<>();
    ThreadLocalRandom.current().longs(10).forEach(x -> {
      keys.add(x);
      client1.put(x, Long.toString(x));
    });
    assertThat(sor.size(), is(10));

    CacheManager anotherCacheManager = newCacheManager(clusterUri, serverResource);
    Cache<Long, String> client2 = anotherCacheManager.createCache("casops" + cacheConsistency.name(),
            getCacheConfig(cacheConsistency, sor));

    keys.forEach(x -> assertThat(client2.putIfAbsent(x, "Again" + x), is(Long.toString(x))));

    assertThat(sor.size(), is(10));

    keys.stream().limit(5).forEach(x ->
            assertThat(client2.replace(x , "Replaced" + x), is(Long.toString(x))));

    assertThat(sor.size(), is(10));

    keys.forEach(x -> client1.remove(x, Long.toString(x)));

    assertThat(sor.size(), is(5));

    AtomicInteger success = new AtomicInteger(0);

    keys.forEach(x -> {
      if (client2.replace(x, "Replaced" + x, "Again")) {
        success.incrementAndGet();
      }
    });

    assertThat(success.get(), is(5));

  }

  private static PersistentCacheManager newCacheManager(URI clusterUri, String serverResource) {
    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager1");
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);
    return CacheManagerBuilder.newCacheManagerBuilder()
      .with(cluster(clusterUri)
        .timeouts(TimeoutsBuilder.timeouts()
          .read(Duration.ofSeconds(30))
          .write(Duration.ofSeconds(30)))
        .autoCreate(c -> c.defaultServerResource(serverResource))
        .build())
      .using(managementRegistry)
      .build(true);
  }

  private CacheConfiguration<Long, String> getCacheConfig(Consistency cacheConsistency, ConcurrentMap<Long, String> sor) {
    return CacheConfigurationBuilder
      .newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder
          .heap(20)
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(2, MemoryUnit.MB)))
      .withLoaderWriter(new TestCacheLoaderWriter(sor))
      .withService(ClusteredStoreConfigurationBuilder.withConsistency(cacheConsistency))
      .withResilienceStrategy(new ThrowingResilienceStrategy<>())
      .build();
  }
}
