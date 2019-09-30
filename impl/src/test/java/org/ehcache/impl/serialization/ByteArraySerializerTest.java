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

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(Randomness.class)
public class ByteArraySerializerTest {

  @RepeatedTest(100)
  public void testCanSerializeAndDeserialize(@Random(size = 64) byte[] bytes) {
    ByteArraySerializer serializer = new ByteArraySerializer();
    byte[] read = serializer.read(serializer.serialize(bytes));
    assertThat(Arrays.equals(read, bytes), is(true));
  }

  @Test
  public void testEquals(@Random(size = 64) byte[] bytes) {
    ByteArraySerializer serializer = new ByteArraySerializer();

    ByteBuffer serialized = serializer.serialize(bytes);

    assertThat(serializer.equals(bytes, serialized), is(true));

    serialized.rewind();

    byte[] read = serializer.read(serialized);
    assertThat(Arrays.equals(read, bytes), is(true));
  }

  @Test
  public void testReadThrowsOnNullInput() {
    assertThrows(NullPointerException.class, () -> new ByteArraySerializer().read(null));
  }

  @Test
  public void testSerializeThrowsOnNullInput() {
    assertThrows(NullPointerException.class, () -> new ByteArraySerializer().serialize(null));
  }
}
