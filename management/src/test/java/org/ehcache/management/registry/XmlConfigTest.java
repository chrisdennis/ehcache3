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
package org.ehcache.management.registry;

import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class XmlConfigTest {

  static class Params implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(
        arguments("ehcache-management-1.xml",
          new DefaultManagementRegistryConfiguration()),
        arguments("ehcache-management-2.xml",
          new DefaultManagementRegistryConfiguration()
            .setCacheManagerAlias("my-cache-manager-name")
            .addTags("webapp-name", "jboss-1", "server-node-1")),
        arguments("ehcache-management-3.xml",
          new DefaultManagementRegistryConfiguration()
            .setCacheManagerAlias("my-cache-manager-name")
            .addTags("webapp-name", "jboss-1", "server-node-1")
            .setCollectorExecutorAlias("my-collectorExecutorAlias")),
        arguments("ehcache-management-4.xml",
          new DefaultManagementRegistryConfiguration()
            .setCacheManagerAlias("my-cache-manager-name")
            .addTags("webapp-name", "jboss-1", "server-node-1")),
        arguments("ehcache-management-5.xml",
          new DefaultManagementRegistryConfiguration()
            .setCacheManagerAlias("my-cache-manager-name")
            .addTags("webapp-name", "jboss-1", "server-node-1"))
      );
    }
  }

  @ParameterizedTest
  @ArgumentsSource(Params.class)
  public void test_config_loaded(String xml, DefaultManagementRegistryConfiguration expectedConfiguration) {
    CacheManager myCacheManager = CacheManagerBuilder.newCacheManager(new XmlConfiguration(getClass().getClassLoader().getResource(xml)));
    myCacheManager.init();
    try {
      DefaultManagementRegistryConfiguration registryConfiguration = null;

      for (ServiceCreationConfiguration<?, ?> configuration : myCacheManager.getRuntimeConfiguration().getServiceCreationConfigurations()) {
        if (configuration instanceof DefaultManagementRegistryConfiguration) {
          registryConfiguration = (DefaultManagementRegistryConfiguration) configuration;
          break;
        }
      }

      assertThat(registryConfiguration, is(not(nullValue())));

      // 1st test: CM alia not set, so generated
      if (xml.endsWith("-1.xml")) {
        expectedConfiguration.setCacheManagerAlias(registryConfiguration.getContext().get("cacheManagerName"));
      }

      assertThat(registryConfiguration.getCacheManagerAlias(), equalTo(expectedConfiguration.getCacheManagerAlias()));
      assertThat(registryConfiguration.getCollectorExecutorAlias(), equalTo(expectedConfiguration.getCollectorExecutorAlias()));
      assertThat(registryConfiguration.getContext(), equalTo(expectedConfiguration.getContext()));
      assertThat(registryConfiguration.getTags(), equalTo(expectedConfiguration.getTags()));

    } finally {
      myCacheManager.close();
    }
  }

}
