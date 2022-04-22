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
package org.ehcache.impl.internal.logging;

import org.ehcache.core.spi.service.LoggingService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class DefaultLoggingService implements LoggingService {

  private final ThreadLocal<Map<String, String>> bindTimeMdc = new ThreadLocal<>();

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {

  }

  @Override
  public void stop() {

  }

  public Context withContext(String key, String value) {
    Map<String, String> context = bindTimeMdc.get();
    if (context == null) {
      bindTimeMdc.set(context = new HashMap<>());
    }
    Map<String, String> finalContext = context;


    String old = finalContext.get(key);
    context.put(key, value);
    if (old == null) {
      return () -> {
         if (finalContext.remove(key, value) && finalContext.isEmpty()) {
           bindTimeMdc.remove();
         }
      };
    } else {
      return () -> finalContext.replace(key, value, old);
    }
  }

  @Override @SuppressWarnings("try")
  public Logger getLogger(Class<?> klazz) {
    Map<String, String> context = bindTimeMdc.get();

    Logger logger = LoggerFactory.getLogger(klazz);

    if (context == null || context.isEmpty()) {
      return logger;
    } else {
      Map<String, String> finalContext = unmodifiableMap(new HashMap<>(context));
      return (Logger) Proxy.newProxyInstance(DefaultLoggingService.class.getClassLoader(), new Class<?>[]{Logger.class}, (proxy, method, args) -> {
        try (Context ignored = applyContext(finalContext)) {
          return method.invoke(logger, args);
        }
      });
    }
  }

  private static Context applyContext(Map<String, String> context) {
    return context.entrySet().stream().<Context>map(e -> {
      String key = e.getKey();
      String old = MDC.get(key);
      MDC.put(key, e.getValue());
      if (old == null) {
        return () -> MDC.remove(key);
      } else {
        return () -> MDC.put(key, old);
      }
    }).reduce(() -> {}, (a, b) -> () -> {
      try {
        a.close();
      } finally {
        b.close();
      }
    });
  }
}
