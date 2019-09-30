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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerValidationException;
import org.ehcache.clustered.client.internal.PassthroughServer;
import org.ehcache.clustered.client.internal.PassthroughServer.Cluster;
import org.ehcache.clustered.client.internal.SimpleClusterTierManagerClientEntity;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

/**
 * This class includes tests to ensure server-side exceptions returned as responses to
 * {@link SimpleClusterTierManagerClientEntity} messages are wrapped before being re-thrown.  This class
 * relies on {@link DefaultClusteringService} to set up conditions for the test and
 * is placed accordingly.
 */
@ExtendWith(PassthroughServer.class)
@PassthroughServer.ServerResource(name = "defaultResource", size = 128)
@PassthroughServer.ServerResource(name = "serverResource1", size = 32)
@PassthroughServer.ServerResource(name = "serverResource2", size = 32)
public class ClusterTierManagerClientEntityExceptionTest {

  /**
   * Tests to ensure that a {@link ClusterException ClusterException}
   * originating in the server is properly wrapped on the client before being re-thrown.
   */
  @Test
  public void testServerExceptionPassThrough(@Cluster URI clusterUri) throws Exception {
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/my-application"))
          .autoCreate(server -> server
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB))
          .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);
    creationService.stop();

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(clusterUri.resolve("/my-application"))
          .expecting(server -> server
            .defaultServerResource("different"))
          .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    /*
     * Induce an "InvalidStoreException: cluster tier 'cacheAlias' does not exist" on the server.
     */
    try {
      accessService.start(null);

      fail("Expecting ClusterTierManagerValidationException");
    } catch (ClusterTierManagerValidationException e) {

      /*
       * Find the last ClusterTierManagerClientEntity involved exception in the causal chain.  This
       * is where the server-side exception should have entered the client.
       */
      Throwable clientSideException = null;
      for (Throwable t = e; t.getCause() != null && t.getCause() != t; t = t.getCause()) {
        for (StackTraceElement element : t.getStackTrace()) {
          if (element.getClassName().endsWith("ClusterTierManagerClientEntity")) {
            clientSideException = t;
          }
        }
      }
      assert clientSideException != null;

      /*
       * In this specific failure case, the exception is expected to be an InvalidStoreException from
       * the server and re-thrown in the client.
       */
      Throwable clientSideCause = clientSideException.getCause();
      assertThat(clientSideCause, is(instanceOf(InvalidServerSideConfigurationException.class)));

      serverCheckLoop:
      {
        for (StackTraceElement element : clientSideCause.getStackTrace()) {
          if (element.getClassName().endsWith("ClusterTierManagerActiveEntity")) {
            break serverCheckLoop;
          }
        }
        fail(clientSideException + " lacks server-based cause");
      }

      assertThat(clientSideException, is(instanceOf(InvalidServerSideConfigurationException.class)));

    } finally {
      accessService.stop();
    }
  }
}
