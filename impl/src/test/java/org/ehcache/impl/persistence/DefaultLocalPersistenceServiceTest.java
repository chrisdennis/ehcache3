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

package org.ehcache.impl.persistence;

import org.ehcache.CachePersistenceException;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.ehcache.impl.internal.util.FileExistenceMatchers.containsCacheDirectory;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.isLocked;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class DefaultLocalPersistenceServiceTest {

  @Test
  public void testFailsIfDirectoryExistsButNotWritable(@TempDir File testFolder) throws IOException {
    assumeTrue(testFolder.setWritable(false));
    try {
      try {
        final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
        service.start(null);
        fail("Expected IllegalArgumentException");
      } catch(IllegalArgumentException e) {
        assertThat(e.getMessage(), equalTo("Location isn't writable: " + testFolder.getAbsolutePath()));
      }
    } finally {
      testFolder.setWritable(true);
    }
  }

  @Test
  public void testFailsIfFileExistsButIsNotDirectory(@TempDir File testFolder) throws IOException {
    File f = new File(testFolder, "testFailsIfFileExistsButIsNotDirectory");
    assertTrue(f.createNewFile());
    try {
      final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(f));
      service.start(null);
      fail("Expected IllegalArgumentException");
    } catch(IllegalArgumentException e) {
      assertThat(e.getMessage(), equalTo("Location is not a directory: " + f.getAbsolutePath()));
    }
  }

  @Test
  public void testFailsIfDirectoryDoesNotExistsAndIsNotCreated(@TempDir File testFolder) throws IOException {
    assumeTrue(testFolder.setWritable(false));
    try {
      File f = new File(testFolder, "notallowed");
      try {
        final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(f));
        service.start(null);
        fail("Expected IllegalArgumentException");
      } catch(IllegalArgumentException e) {
        assertThat(e.getMessage(), equalTo("Directory couldn't be created: " + f.getAbsolutePath()));
      }
    } finally {
      testFolder.setWritable(true);
    }
  }

  @Test
  public void testLocksDirectoryAndUnlocks(@TempDir File testFolder) throws IOException {
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    service.start(null);
    assertThat(service.getLockFile().exists(), is(true));
    service.stop();
    assertThat(service.getLockFile().exists(), is(false));
  }

  @Test
  public void testPhysicalDestroy(@TempDir File testFolder) throws IOException, CachePersistenceException {
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    service.start(null);

    assertThat(service.getLockFile().exists(), is(true));
    assertThat(testFolder, isLocked());

    LocalPersistenceService.SafeSpaceIdentifier id = service.createSafeSpaceIdentifier("test", "test");
    service.createSafeSpace(id);

    assertThat(testFolder, containsCacheDirectory("test", "test"));

    // try to destroy the physical space without the logical id
    LocalPersistenceService.SafeSpaceIdentifier newId = service.createSafeSpaceIdentifier("test", "test");
    service.destroySafeSpace(newId, false);

    assertThat(testFolder, not(containsCacheDirectory("test", "test")));

    service.stop();

    assertThat(testFolder, not(isLocked()));
  }

  @Test
  public void testExclusiveLock(@TempDir File testFolder) throws IOException {
    DefaultLocalPersistenceService service1 = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    DefaultLocalPersistenceService service2 = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    service1.start(null);

    // We should not be able to lock the same directory twice
    // And we should receive a meaningful exception about it
    RuntimeException failure = assertThrows(RuntimeException.class, () -> service2.start(null));
    assertThat(failure.getMessage(), is("Persistence directory already locked by this process: " + testFolder.getAbsolutePath()));
  }
}
