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
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@WithSimpleTerracottaCluster
public class BasicClusteredWriteBehindTest extends WriteBehindTestBase {

  private boolean doThreadDump = true;

  private PersistentCacheManager cacheManager;
  private Cache<Long, String> cache;

  @BeforeEach
  public void setUp(@Cluster URI clusterUri, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    String cacheName = testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new);
    super.setUp();

    cacheManager = createCacheManager(clusterUri, serverResource, cacheName);
    cache = cacheManager.getCache(cacheName, Long.class, String.class);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (doThreadDump) {
      System.out.println("Performing thread dump");
      ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
      ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
      Arrays.stream(threadInfos).forEach(System.out::println);
    }

    if (cacheManager != null) {
      cacheManager.close();
      cacheManager.destroy();
    }
  }

  @Test
  public void testBasicClusteredWriteBehind() throws Exception {
    for (int i = 0; i < 10; i++) {
      cache.put(KEY, String.valueOf(i));
    }

    assertValue(cache, "9");

    checkValueFromLoaderWriter(cache, "9");

    doThreadDump = false;
  }

  @Test
  public void testClusteredWriteBehindCAS() throws Exception {
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
    cache.remove(KEY);

    checkValueFromLoaderWriter(cache, null);

    doThreadDump = false;
  }

  @Test
  public void testClusteredWriteBehindLoading() throws Exception {
    cache.put(KEY, "Some value");
    checkValueFromLoaderWriter(cache, "Some value");
    cache.clear();

    assertThat(cache.get(KEY), notNullValue());

    doThreadDump = false;
  }
}
