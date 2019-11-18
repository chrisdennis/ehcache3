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
package org.ehcache.clustered.client.internal.service;

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.client.internal.PassthroughServer.OffHeapResource;
import org.ehcache.clustered.client.internal.PassthroughServer.WithSimplePassthroughServer;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.terracotta.connection.Connection;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@WithSimplePassthroughServer
public class ConnectionStateTest {

  @Test
  public void testInitializeStateAfterConnectionCloses(@Cluster URI clusterUri) throws Exception {
    ClusteringServiceConfiguration serviceConfiguration = ClusteringServiceConfigurationBuilder
      .cluster(clusterUri.resolve("/cache-manager"))
      .autoCreate(c -> c)
      .build();

    ConnectionState connectionState = new ConnectionState(Timeouts.DEFAULT, new Properties(), serviceConfiguration);
    connectionState.initClusterConnection();

    closeConnection(clusterUri);

    assertThrows(IllegalStateException.class, () -> connectionState.getConnection().close());

    connectionState.initializeState();

    assertThat(connectionState.getConnection(), notNullValue());
    assertThat(connectionState.getEntityFactory(), notNullValue());

    connectionState.getConnection().close();

  }

  @Test
  public void testCreateClusterTierEntityAfterConnectionCloses(@Cluster URI clusterUri, @Cluster String resource) throws Exception {
    ClusteringServiceConfiguration serviceConfiguration = ClusteringServiceConfigurationBuilder
      .cluster(clusterUri.resolve("/cache-manager"))
      .autoCreate(c -> c.defaultServerResource(resource))
      .build();

    ConnectionState connectionState = new ConnectionState(Timeouts.DEFAULT, new Properties(), serviceConfiguration);
    connectionState.initClusterConnection();
    connectionState.initializeState();

    connectionState.getConnection().close();

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB);
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(),
            Long.class.getName(), String.class.getName(), LongSerializer.class.getName(), StringSerializer.class.getName(), null, false);

    ClusterTierClientEntity clientEntity = connectionState.createClusterTierClientEntity("cache1", serverStoreConfiguration, false);

    assertThat(clientEntity, notNullValue());

  }

  //For test to simulate connection close as result of lease expiry
  private void closeConnection(URI clusterUri) throws IOException {
    Collection<Connection> connections = UnitTestConnectionService.getConnections(clusterUri);

    assertThat(connections.size(), is(1));

    Connection connection = connections.iterator().next();

    connection.close();
  }

}
