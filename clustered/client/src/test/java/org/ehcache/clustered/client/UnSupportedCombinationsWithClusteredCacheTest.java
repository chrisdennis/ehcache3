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

package org.ehcache.clustered.client;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;

import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.client.internal.PassthroughServer.WithSimplePassthroughServer;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.transactions.xa.txmgr.btm.BitronixTransactionManagerLookup;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This class should be removed as and when following features are done.
 */
@WithSimplePassthroughServer
public class UnSupportedCombinationsWithClusteredCacheTest {

  @Test
  public void testClusteredCacheWithSynchronousEventListeners(@Cluster URI clusterUri, @Cluster String resource) {
    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(new TestEventListener(), EventType.CREATED, EventType.UPDATED)
        .unordered().synchronous();

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/cache-manager"))
            .autoCreate(c -> c.defaultServerResource(resource)));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
              .with(ClusteredResourcePoolBuilder.clusteredDedicated(8, MemoryUnit.MB)))
          .withService(cacheEventListenerConfiguration)
          .build();

      cacheManager.createCache("test", config);
      fail("IllegalStateException expected");
    } catch (IllegalStateException e){
      assertThat(e.getCause().getMessage(), is("Synchronous CacheEventListener is not supported with clustered tiers"));
    }
    cacheManager.close();
  }

  @Test
  public void testClusteredCacheWithXA(@Cluster URI clusterUri, @Cluster String resource) {
    TransactionManagerServices.getConfiguration().setJournal("null");

    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager();

    try {
      CacheManagerBuilder.newCacheManagerBuilder()
          .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
          .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/cache-manager")).autoCreate(c -> c.defaultServerResource(resource)))
          .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated(8, MemoryUnit.MB))
              )
                  .withService(new XAStoreConfiguration("xaCache"))
                  .build()
          )
          .build(true);
    } catch (StateTransitionException e) {
      assertThat(e.getCause().getCause().getMessage(), is("Unsupported resource type : interface org.ehcache.clustered.client.config.DedicatedClusteredResourcePool"));
    }

    transactionManager.shutdown();
  }

  private static class TestEventListener implements CacheEventListener<Long, String> {

    @Override
    public void onEvent(CacheEvent<? extends Long, ? extends String> event) {

    }
  }

}
