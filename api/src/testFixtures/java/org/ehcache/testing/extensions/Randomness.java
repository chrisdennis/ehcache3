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
package org.ehcache.testing.extensions;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Random;

public class Randomness implements ParameterResolver {

  private static final Namespace NAMESPACE = Namespace.create(Randomness.class);

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(java.util.Random.class) || parameterContext.isAnnotated(Random.class);
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    long seed = extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent("seed", k -> System.nanoTime(), Long.class);

    java.util.Random random = new java.util.Random(seed);


    Class<?> type = parameterContext.getParameter().getType();

    if (type.equals(java.util.Random.class)) {
      reportValue(extensionContext, parameterContext, "new Random(" + seed + ")");
      return random;
    } else if (byte[].class.equals(type)) {
      Random annotation = parameterContext.findAnnotation(Random.class).orElseThrow(() -> new ParameterResolutionException("Unannotated parameter should be impossible"));
      byte[] value = new byte[random.nextInt(annotation.size())];
      random.nextBytes(value);
      reportValue(extensionContext, parameterContext, "byte[" + value.length +"] (seed = " + seed + ")");
      return value;
    } else if (Long.TYPE.equals(type)) {
      return reportValue(extensionContext, parameterContext, random.nextLong());
    } else if (Integer.TYPE.equals(type)) {
      return reportValue(extensionContext, parameterContext, random.nextInt());
    } else if (Character.TYPE.equals(type)) {
      return reportValue(extensionContext, parameterContext, (char) random.nextInt(((int) Character.MAX_VALUE) + 1));
    } else if (Double.TYPE.equals(type)) {
      return reportValue(extensionContext, parameterContext, random.nextDouble());
    } else if (Float.TYPE.equals(type)) {
      return reportValue(extensionContext, parameterContext, random.nextFloat());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private <T> T reportValue(ExtensionContext extensionContext, ParameterContext parameterContext, T value) {
    extensionContext.publishReportEntry("[" + Randomness.class.getName() + "] " + parameterContext.getParameter().toString(), value.toString());
    return value;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.PARAMETER)
  public @interface Random {

    int size() default 1024;
  }
}
