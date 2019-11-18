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

import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.StateTransitionException;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.integration.util.JavaExec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.Serializable;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class PersistentCacheTest {

  @Test
  public void testRecoverPersistentCacheFailsWhenConfiguringIncompatibleClass(@TempDir File folder) throws Exception {
    {
      PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .with(new CacheManagerPersistenceConfiguration(folder))
          .withCache("persistentCache",
              CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                  newResourcePoolsBuilder()
                      .heap(1, MemoryUnit.MB)
                      .offheap(2, MemoryUnit.MB)
                      .disk(5, MemoryUnit.MB, true)
                  )
          ).build(true);


      cacheManager.close();
    }

    {
        PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
            .with(new CacheManagerPersistenceConfiguration(folder))
            .withCache("persistentCache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Serializable.class,
                    newResourcePoolsBuilder()
                        .heap(1, MemoryUnit.MB)
                        .offheap(2, MemoryUnit.MB)
                        .disk(5, MemoryUnit.MB, true)
                    )
            ).build();

      try {
        cacheManager.init();
        fail("expected StateTransitionException");
      } catch (StateTransitionException ste) {
        Throwable rootCause = findRootCause(ste);
        assertThat(rootCause, instanceOf(IllegalArgumentException.class));
        assertThat(rootCause.getMessage(), equalTo("Persisted value type 'java.lang.String' is not the same as the configured value type 'java.io.Serializable'"));
      }
    }
  }

  private Throwable findRootCause(Throwable t) {
    Throwable result = t;
    while (result.getCause() != null) {
      result = result.getCause();
    }
    return result;
  }

  @Test
  public void testRecoverPersistentCacheSucceedsWhenConfiguringArrayClass(@TempDir File folder) throws Exception {
    {
      PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(folder))
        .withCache("persistentCache",
          CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, byte[].class,
            newResourcePoolsBuilder()
              .heap(1, MemoryUnit.MB)
              .offheap(2, MemoryUnit.MB)
              .disk(5, MemoryUnit.MB, true)
          )
        ).build(true);


      cacheManager.close();
    }

    {
      PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(folder))
        .withCache("persistentCache",
          CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, byte[].class,
            newResourcePoolsBuilder()
              .heap(1, MemoryUnit.MB)
              .offheap(2, MemoryUnit.MB)
              .disk(5, MemoryUnit.MB, true)
          )
        ).build(true);


      cacheManager.close();
    }
  }

  @Test
  @SuppressWarnings("try")
  public void testPersistentCachesColliding(@TempDir File folder) throws Exception {
      try (PersistentCacheManager cm = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(folder)).build(true)) {

        StateTransitionException e = assertThrows(StateTransitionException.class, () -> CacheManagerBuilder.newCacheManagerBuilder()
          .with(new CacheManagerPersistenceConfiguration(folder))
          .build(true)
          .close());

        assertThat(e.getCause().getMessage(), containsString("Persistence directory already locked by this process"));
        assertThat(e.getCause().getCause(), instanceOf(OverlappingFileLockException.class));
      }
  }

  @Test
  public void testPersistentCachesCollidingCrossProcess(@TempDir File folder) throws Exception {
    File ping = new File(folder, "ping");
    File pong = new File(folder, "pong");

    Future<Integer> external = JavaExec.exec(Locker.class, folder.getAbsolutePath());
    while (!ping.exists()) ;
    StateTransitionException e = assertThrows(StateTransitionException.class, () -> CacheManagerBuilder.newCacheManagerBuilder().with(new CacheManagerPersistenceConfiguration(folder)).build(true).close());
    assertThat(e.getCause().getMessage(), containsString("Persistence directory already locked by another process"));
  }

  public static final class Locker {

    @SuppressWarnings("try")
    public static void main(String[] args) throws Exception {
      File folder = new File(args[0]);
      File ping = new File(folder, "ping");
      File pong = new File(folder, "pong");

      try (PersistentCacheManager cm = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(folder)).build(true)) {
        ping.createNewFile();
        long bailout = System.nanoTime() + SECONDS.toNanos(30);
        while (System.nanoTime() < bailout && !pong.exists());
      }
    }
  }
}
