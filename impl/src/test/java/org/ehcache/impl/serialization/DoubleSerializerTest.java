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

package org.ehcache.impl.serialization;

import org.ehcache.testing.extensions.Randomness;
import org.ehcache.testing.extensions.Randomness.Random;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(Randomness.class)
public class DoubleSerializerTest {

  @RepeatedTest(100)
  public void testCanSerializeAndDeserialize(@Random double value) throws ClassNotFoundException {
    DoubleSerializer serializer = new DoubleSerializer();
    double read = serializer.read(serializer.serialize(value));
    assertThat(read, is(value));
  }

  @Test
  public void testReadThrowsOnNullInput() {
    assertThrows(NullPointerException.class, () -> new DoubleSerializer().read(null));
  }

  @Test
  public void testSerializeThrowsOnNullInput() {
    assertThrows(NullPointerException.class, () -> new DoubleSerializer().serialize(null));
  }
}
