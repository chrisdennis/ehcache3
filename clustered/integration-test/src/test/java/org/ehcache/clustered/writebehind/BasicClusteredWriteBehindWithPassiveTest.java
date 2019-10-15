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

package org.ehcache.clustered.writebehind;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.testing.extension.TerracottaCluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Topology;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.terracotta.passthrough.IClusterControl;

import java.lang.reflect.Method;
import java.net.URI;

@WithSimpleTerracottaCluster @Topology(2)
@Execution(ExecutionMode.CONCURRENT)
public class BasicClusteredWriteBehindWithPassiveTest extends WriteBehindTestBase {

  private PersistentCacheManager cacheManager;
  private Cache<Long, String> cache;

  @BeforeEach
  public void setUp(@TerracottaCluster.Cluster URI clusterUri, @TerracottaCluster.Cluster IClusterControl clusterControl, @TerracottaCluster.Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new);
    super.setUp();

    clusterControl.startAllServers();
    clusterControl.waitForRunningPassivesInStandby();

    cacheManager = createCacheManager(clusterUri, serverResource, cacheName);
    cache = cacheManager.getCache(cacheName, Long.class, String.class);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testBasicClusteredWriteBehind(@TerracottaCluster.Cluster IClusterControl clusterControl) throws Exception {
    for (int i = 0; i < 10; i++) {
      cache.put(KEY, String.valueOf(i));
    }

    assertValue(cache, "9");

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    assertValue(cache, "9");
    checkValueFromLoaderWriter(cache, String.valueOf(9));
  }

  @Test
  public void testClusteredWriteBehindCAS(@TerracottaCluster.Cluster IClusterControl clusterControl) throws Exception {
    cache.putIfAbsent(KEY, "First value");
    assertValue(cache,"First value");
    cache.putIfAbsent(KEY, "Second value");
    assertValue(cache, "First value");
    cache.put(KEY, "First value again");
    assertValue(cache, "First value again");
    cache.replace(KEY, "Replaced First value");
    assertValue(cache, "Replaced First value");
    cache.replace(KEY, "Replaced First value", "Replaced First value again");
    assertValue(cache, "Replaced First value again");
    cache.replace(KEY, "Replaced First", "Tried Replacing First value again");
    assertValue(cache, "Replaced First value again");
    cache.remove(KEY, "Replaced First value again");
    assertValue(cache, null);
    cache.replace(KEY, "Trying to replace value");
    assertValue(cache, null);
    cache.put(KEY, "new value");
    assertValue(cache, "new value");

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    assertValue(cache, "new value");
    checkValueFromLoaderWriter(cache,"new value");
  }
}
