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

package org.ehcache.clustered.client.internal.lock;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.entity.EntityRef;

import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.READ;
import static org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType.WRITE;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import org.terracotta.exception.EntityNotProvidedException;

@ExtendWith(PassthroughServer.class)
public class VoltronReadWriteLockClientTest {

  private EntityRef<VoltronReadWriteLockClient, Void, Void> getEntityReference(Connection connection) throws EntityNotProvidedException {
    return connection.getEntityRef(VoltronReadWriteLockClient.class, 1, "TestEntity");
  }

  @Test
  public void testWriteLockExcludesRead(@Cluster URI clusterUri) throws Exception {
    try (Connection connection = ConnectionFactory.connect(clusterUri, new Properties())) {
      EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(WRITE);
      try {
        VoltronReadWriteLockClient tester = ref.fetchEntity(null);
        assertThat(tester.tryLock(READ), is(false));
      } finally {
        locker.unlock(WRITE);
      }
    }
  }

  @Test
  public void testWriteLockExcludesWrite(@Cluster URI clusterUri) throws Exception {
    try (Connection connection = ConnectionFactory.connect(clusterUri, new Properties())) {
      EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(WRITE);
      try {
        VoltronReadWriteLockClient tester = ref.fetchEntity(null);
        assertThat(tester.tryLock(WRITE), is(false));
      } finally {
        locker.unlock(WRITE);
      }
    }
  }

  @Test
  public void testReadLockExcludesWrite(@Cluster URI clusterUri) throws Exception {
    try (Connection connection = ConnectionFactory.connect(clusterUri, new Properties())) {
      EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);
      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(READ);
      try {
        VoltronReadWriteLockClient tester = ref.fetchEntity(null);
        assertThat(tester.tryLock(WRITE), is(false));
      } finally {
        locker.unlock(READ);
      }
    }
  }

  @Test
  public void testReadLockAllowsRead(@Cluster URI clusterUri) throws Exception {
    try (Connection connection = ConnectionFactory.connect(clusterUri, new Properties())) {
      EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);
      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(READ);
      try {
        VoltronReadWriteLockClient tester = ref.fetchEntity(null);
        assertThat(tester.tryLock(READ), is(true));
        tester.unlock(READ);
      } finally {
        locker.unlock(READ);
      }
    }
  }

  @Test
  public void testReadUnblocksAfterWriteReleased(@Cluster URI clusterUri) throws Exception {
    try (Connection connection = ConnectionFactory.connect(clusterUri, new Properties())) {
      final EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      Future<Void> success;
      final VoltronReadWriteLockClient tester;

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(WRITE);
      try {
        tester = ref.fetchEntity(null);
        success = async(() -> {
          tester.lock(READ);
          return null;
        });

        try {
          success.get(50, TimeUnit.MILLISECONDS);
          fail("Expected TimeoutException");
        } catch (TimeoutException e) {
          //expected
        }
      } finally {
        locker.unlock(WRITE);
      }

      success.get(2, TimeUnit.MINUTES);
      tester.unlock(READ);
    }
  }

  @Test
  public void testWriteUnblocksAfterWriteReleased(@Cluster URI clusterUri) throws Exception {
    try (Connection connection = ConnectionFactory.connect(clusterUri, new Properties())) {
      final EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      Future<Void> success;
      final VoltronReadWriteLockClient tester;

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(WRITE);
      try {
        tester = ref.fetchEntity(null);
        success = async(() -> {
          tester.lock(WRITE);
          return null;
        });

        try {
          success.get(50, TimeUnit.MILLISECONDS);
          fail("Expected TimeoutException");
        } catch (TimeoutException e) {
          //expected
        }
      } finally {
        locker.unlock(WRITE);
      }

      success.get(2, TimeUnit.MINUTES);
      tester.unlock(WRITE);
    }
  }

  @Test
  public void testWriteUnblocksAfterReadReleased(@Cluster URI clusterUri) throws Exception {
    try (Connection connection = ConnectionFactory.connect(clusterUri, new Properties())) {
      final EntityRef<VoltronReadWriteLockClient, Void, Void> ref = getEntityReference(connection);
      ref.create(null);

      Future<Void> success;
      final VoltronReadWriteLockClient tester;

      VoltronReadWriteLockClient locker = ref.fetchEntity(null);
      locker.lock(READ);
      try {
        tester = ref.fetchEntity(null);
        success = async(() -> {
          tester.lock(WRITE);
          return null;
        });

        try {
          success.get(50, TimeUnit.MILLISECONDS);
          fail("Expected TimeoutException");
        } catch (TimeoutException e) {
          //expected
        }
      } finally {
        locker.unlock(READ);
      }

      success.get(2, TimeUnit.MINUTES);
      tester.unlock(WRITE);
    }
  }

  static <V> Future<V> async(Callable<V> task) {
    ExecutorService e = Executors.newSingleThreadExecutor();
    try {
      return e.submit(task);
    } finally {
      e.shutdown();
    }
  }
}
