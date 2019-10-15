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
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * LongSerializerTest
 */
@ExtendWith(Randomness.class)
public class LongSerializerTest {

  @RepeatedTest(100)
  public void testCanSerializeAndDeserialize(@Randomness.Random long value) throws ClassNotFoundException {
    LongSerializer serializer = new LongSerializer();
    Long read = serializer.read(serializer.serialize(value));
    assertThat(read, is(value));
  }

  @Test
  public void testReadThrowsOnNullInput() {
    assertThrows(NullPointerException.class, () -> new LongSerializer().read(null));
  }

  @Test
  public void testSerializeThrowsOnNullInput() {
    assertThrows(NullPointerException.class, () -> new LongSerializer().serialize(null));
  }
}
