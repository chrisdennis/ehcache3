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
import java.lang.reflect.Array;
import java.net.URI;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class PassthroughServer implements ParameterResolver {

  private static final Namespace NAMESPACE = Namespace.create(PassthroughServer.class);
  private static final String URI_PREFIX = "terracotta://example.com";

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.isAnnotated(Cluster.class) && URI.class.equals(parameterContext.getParameter().getType());
  }

  @Override @SuppressWarnings("unchecked")
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    return extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent("cluster", key -> {

      UnitTestConnectionService.PassthroughServerBuilder builder = new UnitTestConnectionService.PassthroughServerBuilder();
      for (ServerResource resource : getClosestAnnotation(extensionContext, ServerResource.class)) {
        builder = builder.resource(resource.name(), resource.size(), resource.unit());
      }

      for (ClientEntityService service : getClosestAnnotation(extensionContext, ClientEntityService.class)) {
        try {
          builder = builder.clientEntityService(service.value().newInstance());
        } catch (ReflectiveOperationException e) {
          throw new ParameterResolutionException("Could not instantiate service: " + service.value(), e);
        }
      }

      for (ServerEntityService service : getClosestAnnotation(extensionContext, ServerEntityService.class)) {
        try {
          builder = builder.serverEntityService(service.value().newInstance());
        } catch (ReflectiveOperationException e) {
          throw new ParameterResolutionException("Could not instantiate service: " + service.value(), e);
        }
      }

      for (Object service : fieldsAnnotatedWith(extensionContext, ClientEntityService.class)) {
        builder = builder.clientEntityService((EntityClientService<?, ?, ?, ?, ?>) service);
      }

      for (Object service : fieldsAnnotatedWith(extensionContext, ServerEntityService.class)) {
        builder = builder.serverEntityService((EntityServerService<?, ?>) service);
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

  private Iterable<Object> fieldsAnnotatedWith(ExtensionContext extensionContext, Class<? extends Annotation> annotation) {
    Stream.Builder<Class<?>> builder = Stream.builder();
    for (Class<?> clazz = extensionContext.getTestClass().orElse(null); clazz != null; clazz = clazz.getSuperclass()) {
      builder.add(clazz);
    }

    return builder.build().flatMap(k -> Stream.of(k.getDeclaredFields()))
      .filter(field -> field.isAnnotationPresent(annotation))
      .map(field -> {
        try {
          return field.get(extensionContext.getRequiredTestInstance());
        } catch (IllegalAccessException e) {
          throw new ParameterResolutionException("Could not retrieve client service: " + field, e);
        }
      }).collect(toList());
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

  @SuppressWarnings("unchecked")
  private static <T extends Annotation> T[] getClosestAnnotation(ExtensionContext context, Class<T> annotation) {
    while (true) {
      Optional<T[]> serverResources = context.getElement().map(e -> e.getAnnotationsByType(annotation));
      if (serverResources.filter(s -> s.length != 0).isPresent()) {
        return serverResources.get();
      }
      Optional<ExtensionContext> parent = context.getParent();
      if (parent.isPresent()) {
        context = parent.get();
      } else {
        break;
      }
    }
    return (T[]) Array.newInstance(annotation, 0);
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
