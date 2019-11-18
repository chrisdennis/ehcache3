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
package org.ehcache.clustered.testing.extension;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.terracotta.passthrough.IClusterControl;
import org.terracotta.testing.rules.BasicExternalClusterBuilder;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.joining;
import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;
import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;

public class TerracottaCluster implements Extension, BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  @ExtendWith(TerracottaCluster.class)
  @OffHeapResource(name = "primary-server-resource", size = 64)
  public @interface WithSimpleTerracottaCluster {}

  private static final Method BEFORE_METHOD;
  private static final Method AFTER_METHOD;
  static {
    try {
      BEFORE_METHOD = ExternalResource.class.getDeclaredMethod("before");
      AFTER_METHOD = ExternalResource.class.getDeclaredMethod("after");
      BEFORE_METHOD.setAccessible(true);
      AFTER_METHOD.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new AssertionError(e);
    }
  }

  public static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(TerracottaCluster.class);

  @Override
  public void beforeAll(ExtensionContext context) {
    createOrRetrieveCluster(context);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws InterruptedException {
    createOrRetrieveCluster(context).register(context);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    createOrRetrieveCluster(context).unregister(context);
  }

  private ClusterDetails createOrRetrieveCluster(ExtensionContext context) {
    return context.getStore(NAMESPACE).getOrComputeIfAbsent("cluster", k -> startCluster(context), ClusterDetails.class);
  }

  private ClusterDetails startCluster(ExtensionContext context) {
    int[] topology = findAnnotation(context.getElement(), Topology.class).map(Topology::value).orElse(new int[]{1});
    if (topology.length != 1) {
      throw new ExtensionConfigurationException("Cluster must have 1 stripe");
    }

    BasicExternalClusterBuilder clusterBuilder = BasicExternalClusterBuilder.newCluster(topology[0]);

    StringBuilder serviceFragment = new StringBuilder();

    findAnnotation(context.getElement(), ClientLeaseLength.class).map(cll ->
      "<service xmlns:lease='http://www.terracotta.org/service/lease'><lease:connection-leasing>"
      + "<lease:lease-length unit='seconds'>" + cll.value() + "</lease:lease-length>"
      + "</lease:connection-leasing></service>").ifPresent(serviceFragment::append);

    List<OffHeapResource> resources = findRepeatableAnnotations(context.getElement(), OffHeapResource.class);
    serviceFragment.append(resources.stream()
      .map(r -> "<ohr:resource name='" + r.name() + "' unit='MB'>" + r.size() + "</ohr:resource>")
      .collect(joining("\n", "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'><ohr:offheap-resources>", "</ohr:offheap-resources></config>")));

    findRepeatableAnnotations(context.getElement(), SystemProperty.class).forEach(property -> clusterBuilder.withSystemProperty(property.key(), property.value()));

    org.terracotta.testing.rules.Cluster cluster = clusterBuilder.in(Paths.get("build", "cluster")).withServiceFragment(serviceFragment.toString()).build();
    cluster.apply(null, Description.createSuiteDescription(context.getDisplayName()));
    try {
      BEFORE_METHOD.invoke(cluster);
    } catch (ReflectiveOperationException e) {
      throw new ExtensionConfigurationException("Could not initialize cluster", e);
    }
    if (resources.isEmpty()) {
      return new ClusterDetails(cluster, null);
    } else {
      return new ClusterDetails(cluster, resources.get(0).name());
    }
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    if (parameterContext.isAnnotated(Cluster.class)) {
      Class<?> type = parameterContext.getParameter().getType();
      return URI.class.equals(type) || IClusterControl.class.equals(type) || String.class.equals(type);
    } else {
      return false;
    }
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    ClusterDetails cluster = extensionContext.getStore(NAMESPACE).get("cluster", ClusterDetails.class);

    Class<?> type = parameterContext.getParameter().getType();

    if (URI.class.equals(type)) {
      return cluster.getConnectionUri();
    } else if (IClusterControl.class.equals(type)) {
      if (extensionContext.getTestMethod().isPresent()) {
        return cluster.getClusterControl();
      } else {
        return cluster.getDirectClusterControl();
      }
    } else if (String.class.equals(type)) {
      return cluster.getFirstResource();
    } else {
      throw new ParameterResolutionException("Unexpected parameter type: " + type);
    }
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Inherited
  public @interface Topology {
    int[] value() default {1};
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Inherited
  @Repeatable(OffHeapResources.class)
  public @interface OffHeapResource {
    String name();

    int size();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Inherited
  public @interface OffHeapResources {
    OffHeapResource[] value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Inherited
  public @interface ClientLeaseLength {
    int value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Inherited
  @Repeatable(SystemProperties.class)
  public @interface SystemProperty {
    String key();
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Inherited
  public @interface SystemProperties {
    SystemProperty[] value();
  }



  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.PARAMETER)
  public @interface Cluster {}

  class ClusterDetails implements CloseableResource {

    private final org.terracotta.testing.rules.Cluster cluster;
    private final IClusterControl control;
    private final String resource;

    private final Phaser membership = new Phaser() {
      @Override
      protected boolean onAdvance(int phase, int registeredParties) {
        activeCycle.bulkRegister(registeredParties);
        return false;
      }
    };
    private final Phaser activeCycle = new Phaser() {
      @Override
      protected boolean onAdvance(int phase, int registeredParties) {
        return false;
      }
    };

    ClusterDetails(org.terracotta.testing.rules.Cluster cluster, String resource) {
      this.cluster = cluster;
      this.control = new SynchronizedControl(cluster.getClusterControl());
      this.resource = resource;
    }

    public String getFirstResource() {
      return resource;
    }

    URI getConnectionUri() {
      return cluster.getConnectionURI();
    }

    IClusterControl getClusterControl() {
      return control;
    }

    IClusterControl getDirectClusterControl() {
      return cluster.getClusterControl();
    }

    void register(ExtensionContext context) throws InterruptedException {
      membership.register();
      try {
        if (membership.getRegisteredParties() == 1) {
          Thread.sleep(100);
        }
        membership.awaitAdvanceInterruptibly(membership.arrive());
        try {
          activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
        } catch (Throwable t) {
          activeCycle.arriveAndDeregister();
          throw t;
        }
      } catch (Throwable t) {
        membership.arriveAndDeregister();
        throw t;
      }
    }

    void unregister(ExtensionContext context) {
      try {
        activeCycle.arriveAndDeregister();
      } finally {
        membership.arriveAndDeregister();
      }
    }

    @Override
    public void close() {
      try {
        AFTER_METHOD.invoke(cluster);
      } catch (ReflectiveOperationException e) {
        throw new ExtensionConfigurationException("Could not shutdown cluster", e);
      }
    }

    class SynchronizedControl implements IClusterControl {

      private final IClusterControl control;
      private final AtomicReference<ClusterTask> nextTask = new AtomicReference<>();

      SynchronizedControl(IClusterControl control) {
        this.control = control;
      }
      @Override
      public void waitForActive() throws Exception {
        control.waitForActive();
      }

      @Override
      public void waitForRunningPassivesInStandby() throws Exception {
        control.waitForRunningPassivesInStandby();
      }

      @Override
      public void startOneServer() {
        request(ClusterTask.START_ONE_SERVER);
      }

      @Override
      public void startAllServers() {
        request(ClusterTask.START_ALL_SERVERS);
      }

      @Override
      public void terminateActive() {
        request(ClusterTask.TERMINATE_ACTIVE);
      }

      @Override
      public void terminateOnePassive() {
        request(ClusterTask.TERMINATE_ONE_PASSIVE);
      }

      @Override
      public void terminateAllServers() {
        request(ClusterTask.TERMINATE_ALL_SERVERS);
      }

      private void request(ClusterTask task) {
        try {
          if (nextTask.compareAndSet(null, task)) {
            activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
            nextTask.getAndSet(null).accept(control);
            activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
          } else {
            ClusterTask requestedTask = nextTask.get();
            if (requestedTask.equals(task)) {
              activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
              activeCycle.awaitAdvanceInterruptibly(activeCycle.arrive());
            } else {
              throw new AssertionError("Existing requested task is " + requestedTask);
            }
          }
        } catch (Throwable t) {
          throw new AssertionError(t);
        }
      }
    }
  }

  enum ClusterTask implements ThrowingConsumer<IClusterControl> {
    START_ONE_SERVER(IClusterControl::startOneServer),
    START_ALL_SERVERS(IClusterControl::startAllServers),
    TERMINATE_ACTIVE(IClusterControl::terminateActive),
    TERMINATE_ONE_PASSIVE(IClusterControl::terminateOnePassive),
    TERMINATE_ALL_SERVERS(IClusterControl::terminateAllServers);

    private final ThrowingConsumer<IClusterControl> task;

    ClusterTask(ThrowingConsumer<IClusterControl> task) {
      this.task = task;
    }

    public void accept(IClusterControl control) throws Throwable {
      task.accept(control);
    }
  }
}
