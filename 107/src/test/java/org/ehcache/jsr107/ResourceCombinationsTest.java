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

package org.ehcache.jsr107;

import java.io.File;
import java.net.URI;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.cache.CacheManager;

import org.ehcache.config.Builder;
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePools;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.ehcache.jsr107.Eh107Configuration.fromEhcacheCacheConfiguration;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ResourceCombinationsTest {

  static class Params implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
        arguments(newResourcePoolsBuilder().heap(100, ENTRIES)),
        arguments(newResourcePoolsBuilder().offheap(5, MB)),
        arguments(newResourcePoolsBuilder().disk(10, MB)),
        arguments(newResourcePoolsBuilder().heap(100, ENTRIES).offheap(5, MB)),
        arguments(newResourcePoolsBuilder().heap(100, ENTRIES).disk(10, MB)),
        arguments(newResourcePoolsBuilder().heap(100, ENTRIES).offheap(5, MB).disk(10, MB))
      );
    }
  }

  @ParameterizedTest
  @ArgumentsSource(Params.class)
  public void testBasicCacheOperation(Builder<? extends  ResourcePools> resources, @TempDir File persistenceDir) {
    Configuration config = new DefaultConfiguration(ResourceCombinationsTest.class.getClassLoader(),
            new DefaultPersistenceConfiguration(persistenceDir));
    try (CacheManager cacheManager = new EhcacheCachingProvider().getCacheManager(URI.create("dummy"), config)) {
      Cache<String, String> cache = cacheManager.createCache("test", fromEhcacheCacheConfiguration(
        newCacheConfigurationBuilder(String.class, String.class, resources)));
      cache.put("foo", "bar");
      assertThat(cache.get("foo"), is("bar"));
    }
  }
}
