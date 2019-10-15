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
package org.ehcache.clustered.client.internal;

import org.ehcache.config.units.MemoryUnit;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.EntityServerService;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.platform.commons.support.AnnotationSupport.findAnnotatedFieldValues;
import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;
import static org.junit.platform.commons.support.ReflectionSupport.newInstance;

public class PassthroughServer implements ParameterResolver {

  private static final Namespace NAMESPACE = Namespace.create(PassthroughServer.class);
  private static final String URI_PREFIX = "terracotta://example.com";

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.isAnnotated(Cluster.class) && URI.class.equals(parameterContext.getParameter().getType());
  }

  @Override @SuppressWarnings({"unchecked", "rawtypes"})
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    return extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent("cluster", key -> {

      UnitTestConnectionService.PassthroughServerBuilder builder = new UnitTestConnectionService.PassthroughServerBuilder();


      for (ServerResource resource : searchForRepeatableAnnotations(extensionContext, ServerResource.class)) {
        builder = builder.resource(resource.name(), resource.size(), resource.unit());
      }

      for (ServerEntityService service : searchForRepeatableAnnotations(extensionContext, ServerEntityService.class)) {
        builder = builder.serverEntityService(newInstance(service.value()));
      }

      for (ClientEntityService service : searchForRepeatableAnnotations(extensionContext, ClientEntityService.class)) {
        builder = builder.clientEntityService(newInstance(service.value()));
      }

      for (EntityServerService service : extensionContext.getTestClass()
        .map(testClass -> findAnnotatedFieldValues(testClass, ServerEntityService.class, EntityServerService.class))
        .orElse(emptyList())) {
        builder = builder.serverEntityService(service);
      }
      for (EntityServerService service : extensionContext.getTestInstance()
        .map(testInstance -> findAnnotatedFieldValues(testInstance, ServerEntityService.class, EntityServerService.class))
        .orElse(emptyList())) {
        builder = builder.serverEntityService(service);
      }
      for (EntityClientService service : extensionContext.getTestClass()
        .map(testClass -> findAnnotatedFieldValues(testClass, ClientEntityService.class, EntityClientService.class))
        .orElse(emptyList())) {
        builder = builder.clientEntityService(service);
      }
      for (EntityClientService service : extensionContext.getTestInstance()
        .map(testInstance -> findAnnotatedFieldValues(testInstance, ClientEntityService.class, EntityClientService.class))
        .orElse(emptyList())) {
        builder = builder.clientEntityService(service);
      }

      for (int i = 1; i < 1024; i++) {
        try {
          URI uri = URI.create(URI_PREFIX + ":" + i);
          UnitTestConnectionService.add(uri, builder.build());
          extensionContext.publishReportEntry("PassthroughServer", uri.toString());
          return new Holder<URI>(uri) {

            @Override
            public void close() {
              UnitTestConnectionService.remove(get());
            }
          };
        } catch (AssertionError e) {
          //retry
        }
      }
      throw new AssertionError("Too many servers");
    }, Holder.class).get();
  }

  private static <A extends Annotation> List<A> searchForRepeatableAnnotations(ExtensionContext context, Class<A> annoType) {
    Stream.Builder<ExtensionContext> builder = Stream.<ExtensionContext>builder().add(context);

    while (true) {
      Optional<ExtensionContext> parent = context.getParent();
      if (parent.isPresent()) {
        context = parent.get();
        builder.accept(context);
      } else {
        return builder.build().flatMap(c -> findRepeatableAnnotations(c.getElement(), annoType).stream()).collect(toList());
      }
    }
  }

  abstract static class Holder<T> implements ExtensionContext.Store.CloseableResource {

    private final T t;

    Holder(T t) {
      this.t = t;
    }

    T get() {
      return t;
    }
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.PARAMETER)
  public @interface Cluster {}

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Repeatable(ServerResources.class)
  @Inherited
  public @interface ServerResource {
    String name();
    int size();
    MemoryUnit unit() default MemoryUnit.MB;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Inherited
  public @interface ServerResources {
    ServerResource[] value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD})
  @Repeatable(ClientEntityServices.class)
  @Inherited
  public @interface ClientEntityService {
    @SuppressWarnings("rawtypes") Class<? extends EntityClientService> value() default EntityClientService.class;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Inherited
  public @interface ClientEntityServices {
    ClientEntityService[] value();
  }


  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD})
  @Repeatable(ServerEntityServices.class)
  @Inherited
  public @interface ServerEntityService {
    @SuppressWarnings("rawtypes") Class<? extends EntityServerService> value() default EntityServerService.class;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Inherited
  public @interface ServerEntityServices {
    ServerEntityService[] value();
  }
}
