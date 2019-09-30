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

package org.ehcache.integration;

import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.integration.domain.Person;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.persistence;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SerializersTest {

  @Test
  public void testStatefulSerializer(@TempDir File persistenceDir) throws Exception {
    StatefulSerializerImpl<Long> serializer = new StatefulSerializerImpl<>();
    testSerializerWithByRefHeapCache(serializer);
    assertThat(serializer.initCount, is(0));

    serializer = new StatefulSerializerImpl<>();
    testSerializerWithByValueHeapCache(serializer);
    assertThat(serializer.initCount, is(1));

    serializer = new StatefulSerializerImpl<>();
    testSerializerWithOffheapCache(serializer);
    assertThat(serializer.initCount, is(1));

    serializer = new StatefulSerializerImpl<>();
    testSerializerWithHeapOffheapCache(serializer);
    assertThat(serializer.initCount, is(1));

    serializer = new StatefulSerializerImpl<>();
    testSerializerWithDiskCache(serializer, persistenceDir);
    assertThat(serializer.initCount, is(1));

    serializer = new StatefulSerializerImpl<>();
    testSerializerWithHeapDiskCache(serializer, persistenceDir);
    assertThat(serializer.initCount, is(1));

    serializer = new StatefulSerializerImpl<>();
    testSerializerWithThreeTierCache(serializer, persistenceDir);
    assertThat(serializer.initCount, is(1));

  }

  private void testSerializerWithByRefHeapCache(Serializer<Long> serializer) throws Exception {
    CacheManagerBuilder<CacheManager> cmBuilder =
      newCacheManagerBuilder()
        .withCache("heapByRefCache",
          newCacheConfigurationBuilder(Long.class, Person.class, newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
            .withKeySerializer(serializer)
        );
    cmBuilder.build(true).close();
  }

  private void testSerializerWithByValueHeapCache(Serializer<Long> serializer) throws Exception {
    CacheManagerBuilder<CacheManager> cmBuilder =
      newCacheManagerBuilder()
        .withCache("heapByValueCache",
          newCacheConfigurationBuilder(Long.class, Person.class, newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
            .withKeyCopier(SerializingCopier.<Long>asCopierClass())
            .withKeySerializer(serializer)
        );
    cmBuilder.build(true).close();
  }

  private void testSerializerWithOffheapCache(Serializer<Long> serializer) throws Exception {
    CacheManagerBuilder<CacheManager> cmBuilder =
      newCacheManagerBuilder()
        .withCache("offheapCache",
          newCacheConfigurationBuilder(Long.class, Person.class, newResourcePoolsBuilder().offheap(2, MemoryUnit.MB))
            .withKeySerializer(serializer)
        );
    cmBuilder.build(true).close();
  }

  private void testSerializerWithHeapOffheapCache(Serializer<Long> serializer) throws Exception {
    CacheManagerBuilder<CacheManager> cmBuilder =
      newCacheManagerBuilder()
        .withCache("heapOffheapCache",
          newCacheConfigurationBuilder(Long.class, Person.class, newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(2, MemoryUnit.MB))
            .withKeySerializer(serializer)
        );
    cmBuilder.build(true).close();
  }

  private void testSerializerWithDiskCache(Serializer<Long> serializer, File persistenceDir) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> cmBuilder =
      newCacheManagerBuilder()
        .with(persistence(persistenceDir))
        .withCache("diskCache",
          newCacheConfigurationBuilder(Long.class, Person.class, newResourcePoolsBuilder().disk(8, MemoryUnit.MB, true))
            .withKeySerializer(serializer)
        );
    cmBuilder.build(true).close();
  }

  private void testSerializerWithHeapDiskCache(Serializer<Long> serializer, File persistenceDir) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> cmBuilder =
      newCacheManagerBuilder()
        .with(persistence(persistenceDir))
        .withCache("heapDiskCache",
          newCacheConfigurationBuilder(Long.class, Person.class, newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).disk(8, MemoryUnit.MB, true))
            .withKeySerializer(serializer)
        );
    cmBuilder.build(true).close();
  }

  private void testSerializerWithThreeTierCache(Serializer<Long> serializer, File persistenceDir) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> cmBuilder =
      newCacheManagerBuilder()
        .with(persistence(persistenceDir))
        .withCache("heapOffheapDiskCache",
          newCacheConfigurationBuilder(Long.class, Person.class, newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(2, MemoryUnit.MB).disk(8, MemoryUnit.MB, true))
            .withKeySerializer(serializer)
        );
    cmBuilder.build(true).close();
  }

  public static class StatefulSerializerImpl<T> implements StatefulSerializer<T> {

    private int initCount = 0;

    @Override
    public void init(final StateRepository stateRepository) {
      initCount++;
    }

    @Override
    public ByteBuffer serialize(final T object) throws SerializerException {
      return null;
    }

    @Override
    public T read(final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      return null;
    }

    @Override
    public boolean equals(final T object, final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      return false;
    }
  }

}
