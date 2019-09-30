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

package org.ehcache.impl.store;

import org.ehcache.testing.extensions.Randomness;
import org.ehcache.testing.extensions.Randomness.Random;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * HashUtilsTest
 */
@ExtendWith(Randomness.class)
public class HashUtilsTest {

  @RepeatedTest(10)
  public void testHashTransform(@Random int hash) {
    long longHash = HashUtils.intHashToLong(hash);
    int inthash = HashUtils.longHashToInt(longHash);
    assertThat(inthash, is(hash));
  }

}
