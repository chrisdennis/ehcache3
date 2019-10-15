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
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.units.MemoryUnit;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.stream.LongStream.range;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.any;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.fail;

@WithSimpleTerracottaCluster
@Execution(ExecutionMode.CONCURRENT)
public class ClusteredIterationTest extends ClusteredTests {

  @Test
  public void testIterationTerminatedWithException(@Cluster URI clusterUri, @Cluster String serverResource, TestInfo testInfo) {
    try (CacheManager cacheManager = createTestCacheManager(clusterUri, serverResource, testInfo.getDisplayName())) {
      Cache<Long, byte[]> cache = cacheManager.getCache(testInfo.getDisplayName(), Long.class, byte[].class);

      byte[] data = new byte[101 * 1024];
      cache.put(1L, data);
      cache.put(2L, data);

      Iterator<Cache.Entry<Long, byte[]>> iterator = cache.iterator();

      assertThat(iterator.next(), notNullValue());
      assertThat(iterator.next(), notNullValue());

      try {
        iterator.next();
        fail("Expected NoSuchElementException");
      } catch (NoSuchElementException e) {
        //expected
      }
    }
  }

  @Test @SuppressWarnings("unchecked")
  public void testIterationWithSingleLastBatchIsBroken(@Cluster URI clusterUri, @Cluster String serverResource, TestInfo testInfo) {
    try (CacheManager cacheManager = createTestCacheManager(clusterUri, serverResource, testInfo.getDisplayName())) {
      Cache<Long, byte[]> cache = cacheManager.getCache(testInfo.getDisplayName(), Long.class, byte[].class);

      byte[] data = new byte[101 * 1024];
      cache.put(1L, data);
      cache.put(2L, data);

      assertThat(cache, containsInAnyOrder(
        isEntry(is(1L), any(byte[].class)),
        isEntry(is(2L), any(byte[].class))
      ));
    }
  }

  @Test
  public void testIterationWithConcurrentClearedCacheException(@Cluster URI clusterUri, @Cluster String serverResource, TestInfo testInfo) {
    try (CacheManager cacheManager = createTestCacheManager(clusterUri, serverResource, testInfo.getDisplayName())) {
      Cache<Long, byte[]> cache = cacheManager.getCache(testInfo.getDisplayName(), Long.class, byte[].class);

      byte[] data = new byte[10 * 1024];
      Set<Long> initialKeySet = new HashSet<>();
      range(0, 20).forEach(k -> {
        cache.put(k, data);
        initialKeySet.add(k);
      });

      Iterator<Cache.Entry<Long, byte[]>> iterator = cache.iterator();

      cache.clear();

      HashSet<Long> foundKeys = new HashSet<>();
      try {
        while (true) {
          assertThat(foundKeys.add(iterator.next().getKey()), is(true));
        }
      } catch (NoSuchElementException e) {
        //expected
      }
      foundKeys.forEach(k -> assertThat(k, is(in(initialKeySet))));
    }
  }

  private CacheManager createTestCacheManager(URI clusterUri, String serverResource, String cacheName) {
    return newCacheManagerBuilder().with(cluster(clusterUri.resolve("/iteration-cm"))
      .autoCreate(server -> server.defaultServerResource(serverResource)))
      .withCache(cacheName, newCacheConfigurationBuilder(Long.class, byte[].class, newResourcePoolsBuilder()
          .with(clusteredDedicated(1, MemoryUnit.MB)))).build(true);
  }

  private static <K, V> Matcher<Cache.Entry<K, V>> isEntry(Matcher<? super K> keyMatcher, Matcher<? super V> valueMatcher) {
    return new TypeSafeMatcher<Cache.Entry<K, V>>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(" a cache entry { key ").appendDescriptionOf(keyMatcher).appendText(": value ").appendDescriptionOf(valueMatcher).appendText(" }");
      }

      @Override
      protected boolean matchesSafely(Cache.Entry<K, V> item) {
        return keyMatcher.matches(item.getKey()) && valueMatcher.matches(item.getValue());
      }
    };
  }
}
