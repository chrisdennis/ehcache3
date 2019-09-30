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

package org.ehcache.impl.internal.store.offheap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * BasicOffHeapValueHolderTest
 */
public class BasicOffHeapValueHolderTest {

  private String value;
  private BasicOffHeapValueHolder<String> valueHolder;

  @BeforeEach
  public void setUp() {
    value = "aValue";
    valueHolder = new BasicOffHeapValueHolder<>(-1, value, 0, 0);
  }

  @Test
  public void testCanAccessValue() {
    assertThat(valueHolder.get(), is(value));
  }

  @Test
  public void testDoesNotSupportDelayedDeserialization() {
    assertThrows(UnsupportedOperationException.class, valueHolder::detach);
  }

  @Test
  public void testDoesNotSupportForceDeserialization() {
    assertThrows(UnsupportedOperationException.class, valueHolder::forceDeserialization);
  }

  @Test
  public void testDoesNotSupportWriteBack() {
    assertThrows(UnsupportedOperationException.class, valueHolder::writeBack);
  }

  @Test
  public void testDoesNotSupportUpdateMetadata() {
    assertThrows(UnsupportedOperationException.class, () -> valueHolder.updateMetadata(valueHolder));
  }
}
