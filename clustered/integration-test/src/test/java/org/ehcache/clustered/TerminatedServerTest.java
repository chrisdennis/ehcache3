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
import org.ehcache.CacheIterationException;
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.testing.extension.TerracottaCluster.ClientLeaseLength;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import com.tc.net.protocol.transport.ClientMessageTransport;
import com.tc.properties.TCProperties;
import com.tc.properties.TCPropertiesConsts;
import com.tc.properties.TCPropertiesImpl;
import org.terracotta.passthrough.IClusterControl;

import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertTimeout;

/**
 * Provides integration tests in which the server is terminated before the Ehcache operation completes.
 */
// =============================================================================================
// The tests in this class are run **in parallel** to avoid long run times caused by starting
// and stopping a server for each test.  Each test and the environment supporting it must have
// no side effects which can affect another test.
// =============================================================================================
@WithSimpleTerracottaCluster
@ClientLeaseLength(5)
@Execution(ExecutionMode.CONCURRENT)
public class TerminatedServerTest extends ClusteredTests {

  private static final int CLIENT_MAX_PENDING_REQUESTS = 5;

  private static Map<String, String> OLD_PROPERTIES;

  @BeforeAll
  public static void setProperties() {
    Map<String, String> oldProperties = new HashMap<>();

    /*
     * Control for a failed (timed out) connection attempt is not returned until
     * DistributedObjectClient.shutdownResources is complete.  This method attempts to shut down
     * support threads and is subject to a timeout of its own -- tc.properties
     * "l1.shutdown.threadgroup.gracetime" which has a default of 30 seconds -- and is co-dependent on
     * "tc.transport.handshake.timeout" with a default of 10 seconds.  The "tc.transport.handshake.timeout"
     * value is obtained during static initialization of com.tc.net.protocol.transport.ClientMessageTransport
     * -- the change here _may_ not be effective.
     */
    overrideProperty(oldProperties, TCPropertiesConsts.L1_SHUTDOWN_THREADGROUP_GRACETIME, "1000");
    overrideProperty(oldProperties, TCPropertiesConsts.TC_TRANSPORT_HANDSHAKE_TIMEOUT, "1000");

    // Used only by testTerminationFreezesTheClient to be able to fill the inflight queue
    overrideProperty(oldProperties, TCPropertiesConsts.CLIENT_MAX_PENDING_REQUESTS, Integer.toString(CLIENT_MAX_PENDING_REQUESTS));

    OLD_PROPERTIES = oldProperties;
  }

  @AfterAll
  public static void restoreProperties() {
    if (OLD_PROPERTIES != null) {
      TCProperties tcProperties = TCPropertiesImpl.getProperties();
      for (Map.Entry<String, String> entry : OLD_PROPERTIES.entrySet()) {
        tcProperties.setProperty(entry.getKey(), entry.getValue());
      }
    }
  }

  @BeforeEach
  public void restartCluster(@Cluster IClusterControl clusterControl) throws Exception {
    clusterControl.startAllServers();
    clusterControl.waitForRunningPassivesInStandby();
  }

  /**
   * Tests if {@link CacheManager#close()} blocks if the client/server connection is disconnected.
   */
  @Test
  public void testTerminationBeforeCacheManagerClose(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
            .autoCreate(server -> server.defaultServerResource(serverResource)));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    clusterControl.terminateAllServers();

    assertTimeout(ofSeconds(10), cacheManager::close);

    // TODO: Add assertion for successful CacheManager.init() following cluster restart (https://github.com/Terracotta-OSS/galvan/issues/30)
  }

  @Test
  public void testTerminationBeforeCacheManagerCloseWithCaches(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                    .autoCreate(server -> server.defaultServerResource(serverResource)))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    clusterControl.terminateAllServers();

    cacheManager.close();

  }

  @Test
  public void testTerminationBeforeCacheManagerRetrieve(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    // Close all servers
    clusterControl.terminateAllServers();

    // Try to retrieve an entity (that doesn't exist but I don't care... the server is not running anyway
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                    .timeouts(TimeoutsBuilder.timeouts().connection(ofSeconds(1))) // Need a connection timeout shorter than the TimeLimitedTask timeout
                    .expecting(server -> server.defaultServerResource(serverResource)));
    PersistentCacheManager cacheManagerExisting = clusteredCacheManagerBuilder.build(false);

    // Base test time limit on observed TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT; might not have been set in time to be effective
    long synackTimeout = TimeUnit.MILLISECONDS.toSeconds(ClientMessageTransport.TRANSPORT_HANDSHAKE_SYNACK_TIMEOUT);

    assertThatExceptionOfType(StateTransitionException.class).isThrownBy(() ->
      assertTimeout(ofSeconds(synackTimeout + 3), cacheManagerExisting::init)
    ).withRootCauseInstanceOf(TimeoutException.class);
  }

  @Test
  @Disabled("Works but by sending a really low level exception. Need to be fixed to get the expected CachePersistenceException")
  public void testTerminationBeforeCacheManagerDestroyCache(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                    .autoCreate(server -> server.defaultServerResource(serverResource)))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    cacheManager.removeCache("simple-cache");

    clusterControl.terminateAllServers();

    assertThatExceptionOfType(CachePersistenceException.class).isThrownBy(() ->
      assertTimeout(ofSeconds(10), () -> cacheManager.destroyCache("simple-cache"))
    );
  }

  @Test
  @Disabled("There are no timeout on the create cache right now. It waits until the server comes back")
  public void testTerminationBeforeCacheCreate(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                    .autoCreate(server -> server.defaultServerResource(serverResource)));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    clusterControl.terminateAllServers();


    assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> assertTimeout(ofSeconds(10), () ->
      cacheManager.createCache("simple-cache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
            .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))))
    )).withRootCauseInstanceOf(TimeoutException.class);
  }

  @Test
  public void testTerminationBeforeCacheRemove(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                    .autoCreate(server -> server.defaultServerResource(serverResource)))
            .withCache("simple-cache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    clusterControl.terminateAllServers();

    cacheManager.removeCache("simple-cache");
  }

  @Test
  public void testTerminationThenGet(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                .timeouts(TimeoutsBuilder.timeouts().read(Duration.of(1, ChronoUnit.SECONDS)).build())
                .autoCreate(server -> server.defaultServerResource(serverResource)))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    assertThat(cache.get(2L)).isNotNull();

    clusterControl.terminateAllServers();

    String value = assertTimeout(ofSeconds(5), () -> cache.get(2L));

    assertThat(value).isNull();
  }

  @Test
  public void testTerminationThenContainsKey(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                .timeouts(TimeoutsBuilder.timeouts().read(Duration.of(1, ChronoUnit.SECONDS)).build())
                .autoCreate(server -> server.defaultServerResource(serverResource)))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    assertThat(cache.containsKey(2L)).isTrue();

    clusterControl.terminateAllServers();

    boolean value = assertTimeout(ofSeconds(5), () -> cache.containsKey(2L));
    assertThat(value).isFalse();
  }

  @Test
  public void testTerminationThenIterator(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
              .timeouts(TimeoutsBuilder.timeouts().read(Duration.of(1, ChronoUnit.SECONDS)).build())
                .autoCreate(server -> server.defaultServerResource(serverResource)))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    clusterControl.terminateAllServers();

    Iterator<Cache.Entry<Long, String>> value = assertTimeout(ofSeconds(5), cache::iterator);
    assertThat(value.hasNext()).isTrue();
    assertThatExceptionOfType(CacheIterationException.class).isThrownBy(value::next).withRootCauseInstanceOf(TimeoutException.class);
  }

  @Test
  public void testTerminationThenPut(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate(server -> server.defaultServerResource(serverResource)))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    clusterControl.terminateAllServers();

    // The resilience strategy will pick it up and not exception is thrown
    assertTimeout(ofSeconds(10), () -> cache.put(2L, "dos"));
  }

  @Test
  public void testTerminationThenPutIfAbsent(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate(server -> server.defaultServerResource(serverResource)))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    clusterControl.terminateAllServers();

    // The resilience strategy will pick it up and not exception is thrown
    assertTimeout(ofSeconds(10), () -> cache.putIfAbsent(2L, "dos"));
  }

  @Test
  public void testTerminationThenRemove(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate(server -> server.defaultServerResource(serverResource)))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    clusterControl.terminateAllServers();

    assertTimeout(ofSeconds(10), () -> cache.remove(2L));
  }

  @Test
  public void testTerminationThenClear(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    StatisticsService statisticsService = new DefaultStatisticsService();
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .using(statisticsService)
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
                    .timeouts(TimeoutsBuilder.timeouts().write(Duration.of(1, ChronoUnit.SECONDS)).build())
                    .autoCreate(server -> server.defaultServerResource(serverResource)))
        .withCache("simple-cache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    cache.put(1L, "un");
    cache.put(2L, "deux");
    cache.put(3L, "trois");

    clusterControl.terminateAllServers();

    // The resilience strategy will pick it up and not exception is thrown
    assertTimeout(ofSeconds(10), cache::clear);
  }

  /**
   * If the server goes down, the client should not freeze on a server call. It should timeout and answer using
   * the resilience strategy. Whatever the number of calls is done afterwards.
   *
   * @throws Exception
   */
  @Test
  public void testTerminationFreezesTheClient(@Cluster URI clusterUri, @Cluster IClusterControl clusterControl, @Cluster String serverResource, TestInfo testInfo) throws Exception {
    Duration readOperationTimeout = Duration.ofMillis(100);

    try(PersistentCacheManager cacheManager =
          CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/").resolve(testInfo.getTestMethod().map(Method::getName).orElseThrow(AssertionError::new)))
              .timeouts(TimeoutsBuilder.timeouts()
                .read(readOperationTimeout))
              .autoCreate(server -> server.defaultServerResource(serverResource)))
            .withCache("simple-cache",
              CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB))))
            .build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
      cache.put(1L, "un");

      clusterControl.terminateAllServers();

      // Fill the inflight queue and check that we wait no longer than the read timeout
      for (int i = 0; i < CLIENT_MAX_PENDING_REQUESTS; i++) {
        cache.get(1L);
      }

      // The resilience strategy will pick it up and not exception is thrown
      assertTimeout(readOperationTimeout.multipliedBy(2L), () -> cache.get(1L));
    } catch(StateTransitionException e) {
      // On the cacheManager.close(), it waits for the lease to expire and then throw this exception
    }
  }

  private static void overrideProperty(Map<String, String> oldProperties, String propertyName, String propertyValue) {
    TCProperties tcProperties = TCPropertiesImpl.getProperties();
    oldProperties.put(propertyName, tcProperties.getProperty(propertyName, true));
    tcProperties.setProperty(propertyName, propertyValue);
  }
}
