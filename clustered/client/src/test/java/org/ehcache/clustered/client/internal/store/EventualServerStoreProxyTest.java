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
package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.Matchers;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.server.store.ObservableClusterTierServerEntityService.ObservableClusterTierActiveEntity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class EventualServerStoreProxyTest extends AbstractServerStoreProxyTest {

  @Test
  public void testServerSideEvictionFiresInvalidations(@Cluster URI clusterUri) throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity(clusterUri, "testServerSideEvictionFiresInvalidations", Consistency.EVENTUAL, true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity(clusterUri, "testServerSideEvictionFiresInvalidations", Consistency.EVENTUAL, false);

    final List<Long> store1InvalidatedHashes = new CopyOnWriteArrayList<>();
    final List<Long> store2InvalidatedHashes = new CopyOnWriteArrayList<>();

    EventualServerStoreProxy serverStoreProxy1 = new EventualServerStoreProxy("testServerSideEvictionFiresInvalidations", clientEntity1, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash, Chain evictedChain) {
        store1InvalidatedHashes.add(hash);
      }

      @Override
      public void onInvalidateAll() {
        fail("should not be called");
      }

      @Override
      public void onAppend(Chain beforeAppend, ByteBuffer appended) {
        fail("should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });
    EventualServerStoreProxy serverStoreProxy2 = new EventualServerStoreProxy("testServerSideEvictionFiresInvalidations", clientEntity2, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash, Chain evictedChain) {
        store2InvalidatedHashes.add(hash);
      }

      @Override
      public void onInvalidateAll() {
        fail("should not be called");
      }

      @Override
      public void onAppend(Chain beforeAppend, ByteBuffer appended) {
        fail("should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
      }
    });

    final int ITERATIONS = 40;
    for (int i = 0; i < ITERATIONS; i++) {
      serverStoreProxy1.append(i, createPayload(i, 512 * 1024));
    }

    int evictionCount = 0;
    int entryCount = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      Chain elements1 = serverStoreProxy1.get(i);
      Chain elements2 = serverStoreProxy2.get(i);
      assertThat(elements1, Matchers.matchesChain(elements2));
      if (!elements1.isEmpty()) {
        entryCount++;
      } else {
        evictionCount++;
      }
    }

    // there has to be server-side evictions, otherwise this test is useless
    assertThat(store1InvalidatedHashes.size(), greaterThan(0));
    // test that each time the server evicted, the originating client got notified
    assertThat(store1InvalidatedHashes.size(), is(ITERATIONS - entryCount));
    // test that each time the server evicted, the other client got notified on top of normal invalidations
    assertThat(store2InvalidatedHashes.size(), is(ITERATIONS + evictionCount));

    assertThatClientsWaitingForInvalidationIsEmpty("testServerSideEvictionFiresInvalidations");
  }

  @Test
  public void testHashInvalidationListenerWithAppend(@Cluster URI clusterUri) throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity(clusterUri, "testHashInvalidationListenerWithAppend", Consistency.EVENTUAL, true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity(clusterUri,"testHashInvalidationListenerWithAppend", Consistency.EVENTUAL, false);

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Long> invalidatedHash = new AtomicReference<>();


    EventualServerStoreProxy serverStoreProxy1 = new EventualServerStoreProxy("testHashInvalidationListenerWithAppend", clientEntity1, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash, Chain evictedChain) {
        invalidatedHash.set(hash);
        latch.countDown();
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void onAppend(Chain beforeAppend, ByteBuffer appended) {
        fail("should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });
    EventualServerStoreProxy serverStoreProxy2 = new EventualServerStoreProxy("testServerSideEvictionFiresInvalidations", clientEntity2, mock(ServerCallback.class));

    serverStoreProxy2.append(1L, createPayload(1L));

    latch.await(5, TimeUnit.SECONDS);
    assertThat(invalidatedHash.get(), is(1L));
    assertThatClientsWaitingForInvalidationIsEmpty("testHashInvalidationListenerWithAppend");
  }

  @Test
  public void testHashInvalidationListenerWithGetAndAppend(@Cluster URI clusterUri) throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity(clusterUri, "testHashInvalidationListenerWithGetAndAppend", Consistency.EVENTUAL, true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity(clusterUri, "testHashInvalidationListenerWithGetAndAppend", Consistency.EVENTUAL, false);

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Long> invalidatedHash = new AtomicReference<>();


    EventualServerStoreProxy serverStoreProxy1 = new EventualServerStoreProxy("testHashInvalidationListenerWithGetAndAppend", clientEntity1, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash, Chain evictedChain) {
        invalidatedHash.set(hash);
        latch.countDown();
      }

      @Override
      public void onInvalidateAll() {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void onAppend(Chain beforeAppend, ByteBuffer appended) {
        fail("should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });
    EventualServerStoreProxy serverStoreProxy2 = new EventualServerStoreProxy("testHashInvalidationListenerWithGetAndAppend", clientEntity2, mock(ServerCallback.class));

    serverStoreProxy2.getAndAppend(1L, createPayload(1L));

    latch.await(5, TimeUnit.SECONDS);
    assertThat(invalidatedHash.get(), is(1L));
    assertThatClientsWaitingForInvalidationIsEmpty("testHashInvalidationListenerWithGetAndAppend");
  }

  @Test
  public void testAllInvalidationListener(@Cluster URI clusterUri) throws Exception {
    SimpleClusterTierClientEntity clientEntity1 = createClientEntity(clusterUri, "testAllInvalidationListener", Consistency.EVENTUAL, true);
    SimpleClusterTierClientEntity clientEntity2 = createClientEntity(clusterUri, "testAllInvalidationListener", Consistency.EVENTUAL, false);

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean invalidatedAll = new AtomicBoolean();

    EventualServerStoreProxy serverStoreProxy1 = new EventualServerStoreProxy("testAllInvalidationListener", clientEntity1, new ServerCallback() {
      @Override
      public void onInvalidateHash(long hash, Chain evictedChain) {
        throw new AssertionError("Should not be called");
      }

      @Override
      public void onInvalidateAll() {
        invalidatedAll.set(true);
        latch.countDown();
      }

      @Override
      public void onAppend(Chain beforeAppend, ByteBuffer appended) {
        fail("should not be called");
      }

      @Override
      public void compact(ServerStoreProxy.ChainEntry chain) {
        throw new AssertionError();
      }
    });
    EventualServerStoreProxy serverStoreProxy2 = new EventualServerStoreProxy("testAllInvalidationListener", clientEntity2, mock(ServerCallback.class));

    serverStoreProxy2.clear();

    latch.await(5, TimeUnit.SECONDS);
    assertThat(invalidatedAll.get(), is(true));
    assertThatClientsWaitingForInvalidationIsEmpty("testAllInvalidationListener");
  }

  private static void assertThatClientsWaitingForInvalidationIsEmpty(String name) throws Exception {
    ObservableClusterTierActiveEntity activeEntity = observableClusterTierService.getServedActiveEntitiesFor(name).get(0);
    long now = System.currentTimeMillis();
    while (System.currentTimeMillis() < now + 5000 && activeEntity.getClientsWaitingForInvalidation().size() != 0);
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

}
