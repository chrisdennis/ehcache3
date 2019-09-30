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

package org.ehcache.impl.config;

import org.ehcache.config.ResourcePools;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * BaseCacheConfigurationTest
 */
public class BaseCacheConfigurationTest {

  @Test
  public void testThrowsWithNullKeyType() {
    NullPointerException failure = assertThrows(NullPointerException.class, () -> new BaseCacheConfiguration<>(null, String.class, null,
      null, null, mock(ResourcePools.class)));
    assertThat(failure.getMessage(), is("keyType cannot be null"));
  }

  @Test
  public void testThrowsWithNullValueType() {
    NullPointerException failure = assertThrows(NullPointerException.class, () -> new BaseCacheConfiguration<>(Long.class, null, null,
      null, null, mock(ResourcePools.class)));
    assertThat(failure.getMessage(), is("valueType cannot be null"));
  }

  @Test
  public void testThrowsWithNullResourcePools() {
    NullPointerException failure = assertThrows(NullPointerException.class, () -> new BaseCacheConfiguration<>(Long.class, String.class, null,
      null, null, null));
    assertThat(failure.getMessage(), is("resourcePools cannot be null"));
  }

}
