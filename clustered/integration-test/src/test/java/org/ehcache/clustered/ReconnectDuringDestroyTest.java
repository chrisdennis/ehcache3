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
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.client.service.EntityBusyException;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.reconnect.ThrowingResiliencyStrategy;
import org.ehcache.clustered.testing.extension.TerracottaCluster.ClientLeaseLength;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.lease.connection.LeasedConnectionFactory;

import com.tc.net.proxy.TCPProxy;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.clustered.common.EhcacheEntityVersion.ENTITY_VERSION;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * ReconnectDuringDestroyTest
 */
@WithSimpleTerracottaCluster
@ClientLeaseLength(5)
public class ReconnectDuringDestroyTest extends ClusteredTests {

  /*
  This is to test the scenario in which reconnect happens while cache manager
  destruction is in progress. This test checks whether the cache manager
  gets destructed properly in the reconnect path once the connection is closed
  after the prepareForDestroy() call.
  */
  @Test
  public void reconnectDuringDestroyTest(@Cluster URI clusterUri) throws Exception {
    List<TCPProxy> proxies = new ArrayList<>();
    try {
      URI proxyUri = TCPProxyUtil.getProxyURI(clusterUri, proxies);

      Connection client = LeasedConnectionFactory.connect(proxyUri, new Properties());
      VoltronReadWriteLock voltronReadWriteLock = new VoltronReadWriteLock(client, "crud-cm");
      try (VoltronReadWriteLock.Hold localMaintenance = voltronReadWriteLock.tryWriteLock()) {
        if (localMaintenance == null) {
          throw new EntityBusyException("Unable to obtain maintenance lease for " + "crud-cm");
        }
        EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);
        try {
          ClusterTierManagerClientEntity entity = ref.fetchEntity(null);
          entity.prepareForDestroy();
          entity.close();
        } catch (EntityNotFoundException e) {
          fail();
        }
      }
      // For reconnection.
      setDelay(6000, proxies); // Connection Lease time is 5 seconds so delaying for more than 5 seconds.
      Thread.sleep(6000);
      setDelay(0L, proxies);

      try (Connection client2 = LeasedConnectionFactory.connect(proxyUri, new Properties())) {
        // For mimicking the cacheManager.destroy() in the reconnect path.
        voltronReadWriteLock = new VoltronReadWriteLock(client2, "crud-cm");
        try (VoltronReadWriteLock.Hold localMaintenance = voltronReadWriteLock.tryWriteLock()) {
          if (localMaintenance == null) {
            throw new EntityBusyException("Unable to obtain maintenance lease for " + "crud-cm");
          }
          EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client2);
          try {
            ClusterTierManagerClientEntity entity = ref.fetchEntity(null);
            entity.prepareForDestroy();
            entity.close();
          } catch (EntityNotFoundException e) {
            fail("Unexpected exception " + e.getMessage());
          }
          if (!ref.destroy()) {
            fail("Unexpected exception while trying to destroy cache manager");
          }
        }
      }
    } finally {
      proxies.forEach(TCPProxy::stop);
    }
  }

  @Test
  public void reconnectAfterDestroyOneOfTheCache(@Cluster URI clusterUri, @Cluster String serverResource) throws Exception {
    List<TCPProxy> proxies = new ArrayList<>();
    try {
      URI proxyUri = TCPProxyUtil.getProxyURI(clusterUri, proxies);

      ClusteringServiceConfiguration clusteringConfiguration = cluster(proxyUri.resolve("/crud-cm"))
        .autoCreate(server -> server.defaultServerResource(serverResource)).build();

      CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder().with(clusteringConfiguration);
      try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {
        CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
            .with(ClusteredResourcePoolBuilder.
              clusteredDedicated(1, MemoryUnit.MB)))
          .withResilienceStrategy(new ThrowingResiliencyStrategy<>())
          .build();
        Cache<Long, String> cache1 = cacheManager.createCache("clustered-cache-1", config);
        Cache<Long, String> cache2 = cacheManager.createCache("clustered-cache-2", config);
        cache1.put(1L, "The one");
        cache1.put(2L, "The two");
        cache2.put(1L, "The one");
        cache2.put(2L, "The two");
        cacheManager.destroyCache("clustered-cache-1");

        // For reconnection.
        setDelay(6000, proxies); // Connection Lease time is 5 seconds so delaying for more than 5 seconds.
        Thread.sleep(6000);
        setDelay(0L, proxies);

        cache2 = cacheManager.getCache("clustered-cache-2", Long.class, String.class);
        int count = 0;
        while (count < 5) {
          Thread.sleep(2000);
          count++;
          try {
            cache2.get(1L);
            break;
          } catch (Exception e) {
            // Can happen during reconnect
          }
        }
        if (count == 5) {
          fail("Unexpected reconnection exception");
        }
        assertThat(cache2.get(1L), equalTo("The one"));
        assertThat(cache2.get(2L), equalTo("The two"));
        cache2.put(3L, "The three");
        assertThat(cache2.get(3L), equalTo("The three"));
      }
    } finally {
      proxies.forEach(TCPProxy::stop);
    }
  }

  private EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> getEntityRef(Connection client) throws org.terracotta.exception.EntityNotProvidedException {
    return client.getEntityRef(ClusterTierManagerClientEntity.class, ENTITY_VERSION, "crud-cm");
  }
}
