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
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.testing.extension.TerracottaCluster.ClientLeaseLength;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.terracotta.passthrough.IClusterControl;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;

@WithSimpleTerracottaCluster
@ClientLeaseLength(5)
public class CacheManagerDestroyReconnectTest extends ClusteredTests {


  private static PersistentCacheManager cacheManager;

  private static final List<TCPProxy> proxies = new ArrayList<>();

  @BeforeAll
  public static void waitForActive(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource) throws Exception {
    clusterControl.waitForActive();

    URI connectionURI = TCPProxyUtil.getProxyURI(clusterUri, proxies);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
              .autoCreate(server -> server.defaultServerResource(serverResource)));
    cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
  }

  @Test
  public void testDestroyCacheManagerReconnects() throws Exception {

    setDelay(6000, proxies);
    Thread.sleep(6000);

    setDelay(0L, proxies);

    cacheManager.close();

    cacheManager.destroy();

    System.out.println(cacheManager.getStatus());

  }

}
