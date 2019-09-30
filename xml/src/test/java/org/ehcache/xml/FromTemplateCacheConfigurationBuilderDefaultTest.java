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

package org.ehcache.xml;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * TemplateDefaultTest
 */
public class FromTemplateCacheConfigurationBuilderDefaultTest {

  private XmlConfiguration xmlConfiguration;
  private CacheConfigurationBuilder<Object, Object> minimalTemplateBuilder;

  @BeforeEach
  public void setUp() throws Exception {
    xmlConfiguration = new XmlConfiguration(getClass().getResource("/configs/template-defaults.xml"));
    minimalTemplateBuilder = xmlConfiguration.newCacheConfigurationBuilderFromTemplate("minimal-template", Object.class, Object.class, heap(10));
  }

  @Test
  public void testNoConfiguredExpiry() throws Exception {
    assertThat(minimalTemplateBuilder.hasConfiguredExpiry(), is(false));
  }
}
