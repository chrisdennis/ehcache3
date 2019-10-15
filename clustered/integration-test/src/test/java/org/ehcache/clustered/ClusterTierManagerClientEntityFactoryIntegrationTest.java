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

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityFactory;
import org.ehcache.clustered.client.internal.ClusterTierManagerCreationException;
import org.ehcache.clustered.client.internal.ClusterTierManagerValidationException;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.testing.extension.TerracottaCluster.Cluster;
import org.ehcache.clustered.testing.extension.TerracottaCluster.WithSimpleTerracottaCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.terracotta.connection.Connection;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.terracotta.connection.ConnectionFactory.connect;

@WithSimpleTerracottaCluster
@Execution(ExecutionMode.CONCURRENT)
public class ClusterTierManagerClientEntityFactoryIntegrationTest extends ClusteredTests {

  private static final Map<String, Pool> EMPTY_RESOURCE_MAP = Collections.emptyMap();

  private static Connection CONNECTION;

  @BeforeAll
  public static void waitForActive(@Cluster URI clusterUri) throws Exception {
    CONNECTION = connect(clusterUri, new Properties());
  }

  @AfterAll
  public static void closeConnection() throws IOException {
    CONNECTION.close();
  }

  @Test
  public void testCreate() throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);

    factory.create("testCreate", new ServerSideConfiguration(EMPTY_RESOURCE_MAP));
  }

  @Test
  public void testCreateWhenExisting() throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);
    factory.create("testCreateWhenExisting", new ServerSideConfiguration(EMPTY_RESOURCE_MAP));
    try {
      factory.create("testCreateWhenExisting",
          new ServerSideConfiguration(Collections.singletonMap("foo", new Pool(42L, "bar"))));
      fail("Expected EntityAlreadyExistsException");
    } catch (EntityAlreadyExistsException e) {
      //expected
    }
  }

  @Test
  public void testCreateWithBadConfigCleansUp() throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);

    try {
      factory.create("testCreateWithBadConfigCleansUp", new ServerSideConfiguration("flargle", EMPTY_RESOURCE_MAP));
      fail("Expected ClusterTierManagerCreationException");
    } catch (ClusterTierManagerCreationException e) {
      try {
        factory.retrieve("testCreateWithBadConfigCleansUp", null);
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException f) {
        //expected
      }
    }
  }

  @Test
  public void testRetrieveWithGoodConfig(@Cluster String serverResource) throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);
    factory.create("testRetrieveWithGoodConfig",
        new ServerSideConfiguration(Collections.singletonMap("foo", new Pool(43L, serverResource))));
    assertThat(factory.retrieve("testRetrieveWithGoodConfig",
        new ServerSideConfiguration(Collections.singletonMap("foo", new Pool(43L, serverResource)))), notNullValue());
  }

  @Test
  public void testRetrieveWithBadConfig(@Cluster String serverResource) throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);
    factory.create("testRetrieveWithBadConfig",
        new ServerSideConfiguration(Collections.singletonMap("foo", new Pool(42L, serverResource))));
    try {
      factory.retrieve("testRetrieveWithBadConfig",
          new ServerSideConfiguration(Collections.singletonMap("bar", new Pool(42L, serverResource))));
      fail("Expected ClusterTierManagerValidationException");
    } catch (ClusterTierManagerValidationException e) {
      //expected
    }
  }

  @Test
  public void testRetrieveWhenNotExisting() throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);
    try {
      factory.retrieve("testRetrieveWhenNotExisting", null);
      fail("Expected EntityNotFoundException");
    } catch (EntityNotFoundException e) {
      //expected
    }
  }

  @Test
  public void testDestroy() throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);
    factory.create("testDestroy", new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    factory.destroy("testDestroy");
  }

  @Test
  public void testDestroyWhenNotExisting() throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);
    factory.destroy("testDestroyWhenNotExisting");
  }

  @Test
  public void testAbandonLeadershipWhenNotOwning() throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);
    assertFalse(factory.abandonLeadership("testAbandonLeadershipWhenNotOwning", true));
  }

  @Test
  public void testAcquireLeadershipWhenAlone() throws Exception {
    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(CONNECTION);
    assertThat(factory.acquireLeadership("testAcquireLeadershipWhenAlone"), is(true));
  }

  @Test
  public void testAcquireLeadershipWhenTaken(@Cluster URI clusterUri) throws Exception {
    ClusterTierManagerClientEntityFactory factoryA = new ClusterTierManagerClientEntityFactory(CONNECTION);
    assertThat(factoryA.acquireLeadership("testAcquireLeadershipWhenTaken"), is(true));

    try (Connection clientB = connect(clusterUri, new Properties())) {
      ClusterTierManagerClientEntityFactory factoryB = new ClusterTierManagerClientEntityFactory(clientB);
      assertThat(factoryB.acquireLeadership("testAcquireLeadershipWhenTaken"), is(false));
    }
  }

  @Test
  public void testAcquireLeadershipAfterAbandoned(@Cluster URI clusterUri) throws Exception {
    ClusterTierManagerClientEntityFactory factoryA = new ClusterTierManagerClientEntityFactory(CONNECTION);
    factoryA.acquireLeadership("testAcquireLeadershipAfterAbandoned");
    assertTrue(factoryA.abandonLeadership("testAcquireLeadershipAfterAbandoned", true));

    try (Connection clientB = connect(clusterUri, new Properties())) {
      ClusterTierManagerClientEntityFactory factoryB = new ClusterTierManagerClientEntityFactory(clientB);
      assertThat(factoryB.acquireLeadership("testAcquireLeadershipAfterAbandoned"), is(true));
    }
  }
}
