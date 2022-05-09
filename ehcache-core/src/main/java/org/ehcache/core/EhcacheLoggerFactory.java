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
package org.ehcache.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

public class EhcacheLoggerFactory {

  private static final ThreadLocal<Map<String, String>> bindTimeMdc = new ThreadLocal<>();

  public static Context withContext(String key, String value) {
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

  public static Logger getLogger(Class<?> klazz) {
    Map<String, String> context = bindTimeMdc.get();

    Logger logger = LoggerFactory.getLogger(klazz);

    if (context == null || context.isEmpty()) {
      return logger;
    } else {
      String prefix = context.toString();
      return (Logger) Proxy.newProxyInstance(EhcacheLoggerFactory.class.getClassLoader(), new Class<?>[]{Logger.class}, (proxy, method, args) -> {
        Class<?>[] parameterTypes = method.getParameterTypes();
        for (int i =0; i < parameterTypes.length; i++) {
          if (String.class.equals(parameterTypes[i])) {
            args[i] = prefix + " :: " + args[i].toString();
          }
        }
        return method.invoke(logger, args);
      });
    }
  }

  @FunctionalInterface
  interface Context extends AutoCloseable {

    @Override
    void close();
  }
}
