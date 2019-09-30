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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.ehcache.testing.extensions.Randomness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * StringSerializerTest
 */
@ExtendWith(Randomness.class)
public class StringSerializerTest {

  @Test
  public void testSerializeThrowsOnNull() {
    assertThrows(NullPointerException.class, () -> new StringSerializer().serialize(null));
  }

  @Test
  public void testReadThrowsOnNull() {
    assertThrows(NullPointerException.class, () -> new StringSerializer().read(null));
  }

  @Test
  public void testSimpleString(Random random) throws ClassNotFoundException {
    testString(random, "eins");
  }

  @Test
  public void testAllCharacters(Random random) throws ClassNotFoundException {
    char c = Character.MIN_VALUE;
    do {
      testString(random, String.valueOf(c++));
    } while (c != Character.MIN_VALUE);
  }

  private static void testString(Random rndm, String s) throws ClassNotFoundException {
    StringSerializer serializer = new StringSerializer();
    ByteBuffer serialized = serializer.serialize(s);

    String read = serializer.read(serialized.asReadOnlyBuffer());
    assertThat(read, is(s));

    assertThat(serializer.equals(s, serialized), is(true));

    String padded = s + (char) rndm.nextInt();
    assertThat(serializer.equals(padded, serialized.asReadOnlyBuffer()), is(false));

    String trimmed = s.substring(0, s.length() - 1);
    assertThat(serializer.equals(trimmed, serialized.asReadOnlyBuffer()), is(false));

    char target = s.charAt(rndm.nextInt(s.length()));
    char replacement;
    do {
      replacement = (char) rndm.nextInt();
    } while (replacement == target);

    String mutated = s.replace(target, replacement);
    assertThat(serializer.equals(mutated, serialized.asReadOnlyBuffer()), is(false));
  }

  @Test
  public void testBackwardsCompatibility() throws UnsupportedEncodingException, ClassNotFoundException {
    StringSerializer serializer = new StringSerializer();
    int codepoint = 65536;
    do {
      if (Character.isValidCodePoint(codepoint) && !(Character.isHighSurrogate((char) codepoint) || Character.isLowSurrogate((char) codepoint))) {
        String s = new String(Character.toChars(codepoint));
        ByteBuffer bytes = ByteBuffer.wrap(s.getBytes("UTF-8"));
        assertThat("Codepoint : 0x" + Integer.toHexString(codepoint), serializer.read(bytes), is(s));
        assertThat("Codepoint : 0x" + Integer.toHexString(codepoint), serializer.equals(s, bytes), is(true));
      }
    } while (++codepoint != Integer.MIN_VALUE);
  }

  @Test
  public void testEqualsMismatchOnMissingFinalSurrogateAgainstOldFormat() throws UnsupportedEncodingException, ClassNotFoundException {
    StringSerializer serializer = new StringSerializer();
    String string = "🂱🂾🂽🂻🂺";

    String trimmed = string.substring(0, string.length() - 1);
    ByteBuffer bytes = ByteBuffer.wrap(string.getBytes("UTF-8"));
    assertThat(serializer.equals(trimmed, bytes), is(false));
  }
}
