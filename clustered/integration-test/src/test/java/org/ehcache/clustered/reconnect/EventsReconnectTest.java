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
package org.ehcache.clustered.reconnect;

import com.tc.net.proxy.TCPProxy;
import org.awaitility.Durations;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.store.ReconnectInProgressException;
import org.ehcache.clustered.testing.extension.TerracottaCluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.terracotta.passthrough.IClusterControl;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

@TerracottaCluster.WithSimpleTerracottaCluster
@TerracottaCluster.ClientLeaseLength(5)
public class EventsReconnectTest extends ClusteredTests {
  private static final Duration TIMEOUT = Durations.FIVE_SECONDS;

  private static PersistentCacheManager cacheManager;

  private static class AccountingCacheEventListener<K, V> implements CacheEventListener<K, V> {
    private final Map<EventType, List<CacheEvent<? extends K, ? extends V>>> events;

    AccountingCacheEventListener() {
      events = new HashMap<>();
      clear();
    }

    @Override
    public void onEvent(CacheEvent<? extends K, ? extends V> event) {
      events.get(event.getType()).add(event);
    }

    final void clear() {
      for (EventType value : EventType.values()) {
        events.put(value, new CopyOnWriteArrayList<>());
      }
    }

  }

  private static AccountingCacheEventListener<Long, String> cacheEventListener = new AccountingCacheEventListener<>();

  private static CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
    ResourcePoolsBuilder.newResourcePoolsBuilder()
      .with(ClusteredResourcePoolBuilder.clusteredDedicated(1, MemoryUnit.MB)))
    .withService(CacheEventListenerConfigurationBuilder
      .newEventListenerConfiguration(cacheEventListener, EnumSet.allOf(EventType.class))
      .unordered().asynchronous())
    .withResilienceStrategy(new ThrowingResiliencyStrategy<>())
    .build();

  private static final List<TCPProxy> proxies = new ArrayList<>();

  @BeforeAll
  public static void waitForActive(@Cluster URI clusterUri, @Cluster String serverResource) throws Exception {
    URI connectionURI = TCPProxyUtil.getProxyURI(clusterUri, proxies);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
                    .autoCreate(s -> s.defaultServerResource(serverResource)));
    cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
  }

  @Test
  public void eventsFlowAgainAfterReconnection() throws Exception {
    try {
      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

      CompletableFuture<Void> future = CompletableFuture.runAsync(() ->
              ThreadLocalRandom.current()
                      .longs()
                      .filter(val -> val != Long.MAX_VALUE)
                      .forEach(value ->
                              cache.put(value, Long.toString(value))));

      expireLease();

      try {
        future.get(5000, TimeUnit.MILLISECONDS);
        fail();
      } catch (ExecutionException e) {
        assertThat(e.getCause().getCause().getCause(), instanceOf(ReconnectInProgressException.class));
      }
      int beforeDisconnectionEventCounter = cacheEventListener.events.get(EventType.CREATED).size();

      CompletableFuture<Void> getSucceededFuture = CompletableFuture.runAsync(() -> {
        while (true) {
          try {
            cache.put(Long.MAX_VALUE, "");
            break;
          } catch (RuntimeException e) {

          }
        }
      });

      getSucceededFuture.get(20000, TimeUnit.MILLISECONDS);

      await().atMost(TIMEOUT).until(() -> cacheEventListener.events.get(EventType.CREATED).size(), is(beforeDisconnectionEventCounter + 1));

    } finally {
      cacheManager.destroyCache("clustered-cache");
    }
  }


  private static void expireLease() throws InterruptedException {
    setDelay(6000, proxies);
    Thread.sleep(6000);

    setDelay(0L, proxies);
  }
}
