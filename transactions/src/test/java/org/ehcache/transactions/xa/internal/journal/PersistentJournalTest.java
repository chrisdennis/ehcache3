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

package org.ehcache.transactions.xa.internal.journal;

import org.ehcache.transactions.xa.internal.TransactionId;
import org.ehcache.transactions.xa.utils.JavaSerializer;
import org.ehcache.transactions.xa.utils.TestXid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Ludovic Orban
 */
@ExtendWith(PersistentJournalTest.JournalResolver.class)
public class PersistentJournalTest extends AbstractJournalTest {

  private static final String KEY = "temp.dir";
  private static final String TEMP_DIR_PREFIX = "junit-journal";

  static class JournalResolver implements ParameterResolver {

    private static final Namespace NAMESPACE = Namespace.create(JournalResolver.class);

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
      return parameterContext.getParameter().getType().isAssignableFrom(PersistentJournal.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
      return new PersistentJournal<>(getJournalPathFor(extensionContext).toFile(), new JavaSerializer<>(ClassLoader.getSystemClassLoader()));
    }

    private Path getJournalPathFor(ExtensionContext extensionContext) {
      Path path = extensionContext.getStore(NAMESPACE) //
        .getOrComputeIfAbsent(KEY, key -> createTempDir(), CloseablePath.class) //
        .get();

      return path;

    }

    private static CloseablePath createTempDir() {
      try {
        return new CloseablePath(Files.createTempDirectory(TEMP_DIR_PREFIX));
      } catch (Exception ex) {
        throw new ExtensionConfigurationException("Failed to create journal directory", ex);
      }
    }

    private static class CloseablePath implements ExtensionContext.Store.CloseableResource {

      private final Path dir;

      CloseablePath(Path dir) {
        this.dir = dir;
      }

      Path get() {
        return dir;
      }

      @Override
      public void close() throws IOException {
        SortedMap<Path, IOException> failures = deleteAllFilesAndDirectories();
        if (!failures.isEmpty()) {
          throw createIOExceptionWithAttachedFailures(failures);
        }
      }

      private SortedMap<Path, IOException> deleteAllFilesAndDirectories() throws IOException {
        if (Files.notExists(dir)) {
          return Collections.emptySortedMap();
        }

        SortedMap<Path, IOException> failures = new TreeMap<>();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
            return deleteAndContinue(file);
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
            return deleteAndContinue(dir);
          }

          private FileVisitResult deleteAndContinue(Path path) {
            try {
              Files.delete(path);
            }
            catch (IOException ex) {
              failures.put(path, ex);
            }
            return CONTINUE;
          }
        });
        return failures;
      }

      private IOException createIOExceptionWithAttachedFailures(SortedMap<Path, IOException> failures) {
        // @formatter:off
        String joinedPaths = failures.keySet().stream()
          .peek(this::tryToDeleteOnExit)
          .map(this::relativizeSafely)
          .map(String::valueOf)
          .collect(joining(", "));
        // @formatter:on
        IOException exception = new IOException("Failed to delete temp directory " + dir.toAbsolutePath()
          + ". The following paths could not be deleted (see suppressed exceptions for details): "
          + joinedPaths);
        failures.values().forEach(exception::addSuppressed);
        return exception;
      }

      private void tryToDeleteOnExit(Path path) {
        try {
          path.toFile().deleteOnExit();
        }
        catch (UnsupportedOperationException ignore) {
        }
      }

      private Path relativizeSafely(Path path) {
        try {
          return dir.relativize(path);
        }
        catch (IllegalArgumentException e) {
          return path;
        }
      }
    }


  }

  @Test
  public void testPersistence(PersistentJournal<Long> journal) throws Exception {
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(1L, 2L, 3L));
    journal.saveInDoubt(new TransactionId(new TestXid(1, 0)), Arrays.asList(4L, 5L, 6L));
    journal.saveCommitted(new TransactionId(new TestXid(1, 0)), true);

    journal.close();


    journal = new PersistentJournal<>(journal.getDirectory(), new JavaSerializer<>(ClassLoader.getSystemClassLoader()));
    journal.open();

    assertThat(journal.recover().keySet(), containsInAnyOrder(new TransactionId(new TestXid(0, 0))));
    assertThat(journal.heuristicDecisions().keySet(), containsInAnyOrder(new TransactionId(new TestXid(1, 0))));

    journal.saveRolledBack(new TransactionId(new TestXid(0, 0)), false);
    journal.forget(new TransactionId(new TestXid(1, 0)));

    journal.close();
    journal = new PersistentJournal<>(journal.getDirectory(), new JavaSerializer<>(ClassLoader.getSystemClassLoader()));
    journal.open();

    assertThat(journal.recover().isEmpty(), is(true));
    assertThat(journal.heuristicDecisions().isEmpty(), is(true));
  }
}
