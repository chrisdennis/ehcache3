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

package org.ehcache.clustered.client.docs;

import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.client.internal.PassthroughServer.ServerResource;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.time.Duration;

@ExtendWith(PassthroughServer.class)
@ServerResource(name = "primary-server-resource", size = 128)
public class Resilience {

  @Test
  public void clusteredCacheManagerExample(@Cluster URI clusterUri) throws Exception {
    // tag::timeoutsExample[]
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
      CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/my-application"))
          .timeouts(TimeoutsBuilder.timeouts() // <1>
            .read(Duration.ofSeconds(10)) // <2>
            .write(Timeouts.DEFAULT_OPERATION_TIMEOUT) // <3>
            .connection(Timeouts.INFINITE_TIMEOUT)) // <4>
          .autoCreate(c -> c));
    // end::timeoutsExample[]
  }
}
