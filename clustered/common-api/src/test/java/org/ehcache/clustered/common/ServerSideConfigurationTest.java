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

package org.ehcache.clustered.common;

import java.util.Collections;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ServerSideConfigurationTest {

  @Test
  public void testNullDefaultPoolThrowsNPE() {
    assertThrows(NullPointerException.class, () -> new ServerSideConfiguration(null, Collections.<String, Pool>emptyMap()));
  }

  @Test
  public void testPoolUsingDefaultWithNoDefaultThrowsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new ServerSideConfiguration(Collections.singletonMap("foo", new Pool(1))));
  }

  @Test
  public void testDefaultPoolWithIllegalSize() {
    assertThrows(IllegalArgumentException.class, () -> new Pool(0));
  }

  @Test
  public void testPoolWithIllegalSize() {
    assertThrows(IllegalArgumentException.class, () -> new Pool(0, "foo"));
  }
}
