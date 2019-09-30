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

package org.ehcache.impl.internal.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.ehcache.impl.internal.util.FileExistenceMatchers.containsCacheDirectory;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.isLocked;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Henri Tremblay
 */
public class FileExistenceMatchersTest {

  @Test
  public void directoryIsLocked(@TempDir File dir) {
    assertThat(dir, not(isLocked()));
  }

  @Test
  public void directoryIsNotLocked(@TempDir File dir) throws IOException {
    File lock = new File(dir, ".lock");
    lock.createNewFile();

    assertThat(dir, isLocked());
  }

  @Test
  public void containsCacheDirectory_noFileDir(@TempDir File dir) {
    assertThat(dir, not(containsCacheDirectory("test123")));
  }

  @Test
  public void containsCacheDirectory_noCacheDir(@TempDir File dir) {
    File file = new File(dir, "file");
    file.mkdir();

    assertThat(dir, not(containsCacheDirectory("test123")));
  }

  @Test
  public void containsCacheDirectory_moreThanOneCacheDir(@TempDir File dir) {
    File file = new File(dir, "file");
    file.mkdir();
    new File(file, "test123_aaa").mkdir();
    new File(file, "test123_bbb").mkdir();

    assertThat(dir, not(containsCacheDirectory("test123")));
  }

  @Test
  public void containsCacheDirectory_existing(@TempDir File dir) {
    new File(dir, "file/test123_aaa").mkdirs();

    assertThat(dir, containsCacheDirectory("test123"));
  }

  @Test
  public void containsCacheDirectory_withSafeSpaceExisting(@TempDir File dir) {
    new File(dir, "safespace/test123_aaa").mkdirs();

    assertThat(dir, containsCacheDirectory("safespace", "test123"));
  }
}
