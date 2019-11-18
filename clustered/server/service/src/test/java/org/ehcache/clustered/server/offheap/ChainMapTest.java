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
package org.ehcache.clustered.server.offheap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.StringPortability;

import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;

@SuppressWarnings("unchecked") // To replace by @SafeVarargs in JDK7
public class ChainMapTest {

  @ParameterizedTest
  @ArgumentsSource(Params.class)
  @interface TestAllParams {}

  static class Params implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(
        arguments(false, 4096, 4096),
        arguments(true, 4096, 4096),
        arguments(false, 128, 4096)
      );
    }
  }

  @TestAllParams
  public void testInitiallyEmptyChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);

    assertThat(map.get("foo"), emptyIterable());

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testAppendToEmptyChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);

    map.append("foo", buffer(1));
    assertThat(map.get("foo"), contains(element(1)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testGetAndAppendToEmptyChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);

    assertThat(map.getAndAppend("foo", buffer(1)), emptyIterable());
    assertThat(map.get("foo"), contains(element(1)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testAppendToSingletonChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    map.append("foo", buffer(2));
    assertThat(map.get("foo"), contains(element(1), element(2)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testGetAndAppendToSingletonChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    assertThat(map.getAndAppend("foo", buffer(2)), contains(element(1)));
    assertThat(map.get("foo"), contains(element(1), element(2)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testAppendToDoubleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    map.append("foo", buffer(3));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testGetAndAppendToDoubleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    assertThat(map.getAndAppend("foo", buffer(3)), contains(element(1), element(2)));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testAppendToTripleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    map.append("foo", buffer(4));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3), element(4)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testGetAndAppendToTripleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    assertThat(map.getAndAppend("foo", buffer(4)), contains(element(1), element(2), element(3)));
    assertThat(map.get("foo"), contains(element(1), element(2), element(3), element(4)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testReplaceEmptyChainAtHeadOnEmptyChainFails(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);

    assertThrows(IllegalArgumentException.class, () -> map.replaceAtHead("foo", chainOf(), chainOf(buffer(1))));
    emptyAndValidate(map);
  }

  @TestAllParams
  public void testReplaceEmptyChainAtHeadOnNonEmptyChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    assertThrows(IllegalArgumentException.class, () -> map.replaceAtHead("foo", chainOf(), chainOf(buffer(2))));
    emptyAndValidate(map);
  }

  @TestAllParams
  public void testMismatchingReplaceSingletonChainAtHeadOnSingletonChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    map.replaceAtHead("foo", chainOf(buffer(2)), chainOf(buffer(42)));
    assertThat(map.get("foo"), contains(element(1)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testReplaceSingletonChainAtHeadOnSingletonChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));

    map.replaceAtHead("foo", chainOf(buffer(1)), chainOf(buffer(42)));
    assertThat(map.get("foo"), contains(element(42)));

     emptyAndValidate(map);
  }

  @TestAllParams
  public void testReplaceSingletonChainAtHeadOnDoubleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    map.replaceAtHead("foo", chainOf(buffer(1)), chainOf(buffer(42)));
    assertThat(map.get("foo"), contains(element(42), element(2)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testReplaceSingletonChainAtHeadOnTripleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    map.replaceAtHead("foo", chainOf(buffer(1)), chainOf(buffer(42)));
    assertThat(map.get("foo"), contains(element(42), element(2), element(3)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testMismatchingReplacePluralChainAtHead(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    map.replaceAtHead("foo", chainOf(buffer(1), buffer(3)), chainOf(buffer(42)));
    assertThat(map.get("foo"), contains(element(1), element(2)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testReplacePluralChainAtHeadOnDoubleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));

    map.replaceAtHead("foo", chainOf(buffer(1), buffer(2)), chainOf(buffer(42)));
    assertThat(map.get("foo"), contains(element(42)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testReplacePluralChainAtHeadOnTripleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    map.replaceAtHead("foo", chainOf(buffer(1), buffer(2)), chainOf(buffer(42)));
    assertThat(map.get("foo"), contains(element(42), element(3)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testReplacePluralChainAtHeadWithEmpty(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    long before = map.getDataOccupiedMemory();
    map.replaceAtHead("foo", chainOf(buffer(1), buffer(2)), chainOf());
    assertThat(map.getDataOccupiedMemory(), lessThan(before));
    assertThat(map.get("foo"), contains(element(3)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testSequenceBasedChainComparison(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    map.replaceAtHead("foo", map.get("foo"), chainOf());
    assertThat(map.get("foo"), emptyIterable());

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testReplaceFullPluralChainAtHeadWithEmpty(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    map.append("foo", buffer(3));

    assertThat(map.getDataOccupiedMemory(), greaterThan(0L));
    map.replaceAtHead("foo", chainOf(buffer(1), buffer(2), buffer(3)), chainOf());
    assertThat(map.getDataOccupiedMemory(), is(0L));
    assertThat(map.get("foo"), emptyIterable());

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testContinualAppendCausingEvictionIsStable(boolean steal, int minPageSize, int maxPageSize) {
    UpfrontAllocatingPageSource pageSource = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), KILOBYTES.toBytes(1024L), KILOBYTES.toBytes(1024));
    if (steal) {
      OffHeapChainMap<String> mapA = new OffHeapChainMap<>(pageSource, StringPortability.INSTANCE, minPageSize, maxPageSize, true);
      OffHeapChainMap<String> mapB = new OffHeapChainMap<>(pageSource, StringPortability.INSTANCE, minPageSize, maxPageSize, true);

      for (int c = 0; ; c++) {
        long before = mapA.getOccupiedMemory();
        for (int i = 0; i < 100; i++) {
            mapA.append(Integer.toString(i), buffer(2));
            mapB.append(Integer.toString(i), buffer(2));
        }
        if (mapA.getOccupiedMemory() <= before) {
          while (c-- > 0) {
            for (int i = 0; i < 100; i++) {
                mapA.append(Integer.toString(i), buffer(2));
                mapB.append(Integer.toString(i), buffer(2));
            }
          }
          break;
        }
      }

      emptyAndValidate(mapA);
      emptyAndValidate(mapB);
    } else {
      OffHeapChainMap<String> map = new OffHeapChainMap<>(pageSource, StringPortability.INSTANCE, minPageSize, maxPageSize, false);

      for (int c = 0; ; c++) {
        long before = map.getOccupiedMemory();
        for (int i = 0; i < 100; i++) {
            map.append(Integer.toString(i), buffer(2));
        }
        if (map.getOccupiedMemory() <= before) {
          while (c-- > 0) {
            for (int i = 0; i < 100; i++) {
                map.append(Integer.toString(i), buffer(2));
            }
          }
          break;
        }
      }

      emptyAndValidate(map);
    }
  }

  @TestAllParams
  public void testPutWhenKeyIsNotNull(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("key", buffer(3));
    map.put("key", chainOf(buffer(1), buffer(2)));

    assertThat(map.get("key"), contains(element(1), element(2)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testPutWhenKeyIsNull(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.put("key", chainOf(buffer(1), buffer(2)));

    assertThat(map.get("key"), contains(element(1), element(2)));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testActiveChainsThreadSafety(boolean steal, int minPageSize, int maxPageSize) throws ExecutionException, InterruptedException {
    UnlimitedPageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    OffHeapChainStorageEngine<String> chainStorage = new OffHeapChainStorageEngine<>(source, StringPortability.INSTANCE, minPageSize, maxPageSize, steal, steal);

    OffHeapChainMap.HeadMap<String> heads = new OffHeapChainMap.HeadMap<>(callable -> {}, source, chainStorage);

    OffHeapChainMap<String> map = new OffHeapChainMap<>(heads, chainStorage);

    map.put("key", chainOf(buffer(1), buffer(2)));

    int nThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

    List<Future<Chain>> futures = new ArrayList<>();

    for (int i = 0; i < nThreads ; i++) {
      futures.add(executorService.submit(() -> map.get("key")));
    }

    for (Future<Chain> f : futures) {
      f.get();
    }

    assertThat(chainStorage.getActiveChains().size(), is(0));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testPutDoesNotLeakWhenMappingIsNotNull(boolean steal, int minPageSize, int maxPageSize) {
    UnlimitedPageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    OffHeapChainStorageEngine<String> chainStorage = new OffHeapChainStorageEngine<>(source, StringPortability.INSTANCE, minPageSize, maxPageSize, steal, steal);

    OffHeapChainMap.HeadMap<String> heads = new OffHeapChainMap.HeadMap<>(callable -> {}, source, chainStorage);

    OffHeapChainMap<String> map = new OffHeapChainMap<>(heads, chainStorage);

    map.put("key", chainOf(buffer(1)));
    map.put("key", chainOf(buffer(2)));

    assertThat(chainStorage.getActiveChains().size(), is(0));

    emptyAndValidate(map);
  }

  @TestAllParams
  public void testRemoveMissingKey(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.remove("foo");
    assertThat(map.get("foo").isEmpty(), is(true));
  }

  @TestAllParams
  public void testRemoveSingleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("bar", buffer(2));
    assertThat(map.get("foo"), contains(element(1)));
    assertThat(map.get("bar"), contains(element(2)));

    map.remove("foo");
    assertThat(map.get("foo").isEmpty(), is(true));
    assertThat(map.get("bar"), contains(element(2)));
  }

  @TestAllParams
  public void testRemoveDoubleChain(boolean steal, int minPageSize, int maxPageSize) {
    OffHeapChainMap<String> map = new OffHeapChainMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), StringPortability.INSTANCE, minPageSize, maxPageSize, steal);
    map.append("foo", buffer(1));
    map.append("foo", buffer(2));
    assertThat(map.get("foo"), contains(element(1), element(2)));

    map.remove("foo");
    assertThat(map.get("foo").isEmpty(), is(true));
  }

  private static ByteBuffer buffer(int i) {
    ByteBuffer buffer = ByteBuffer.allocate(i);
    while (buffer.hasRemaining()) {
      buffer.put((byte) i);
    }
    return (ByteBuffer) buffer.flip();
  }

  private static Matcher<Element> element(final int i) {
    return new TypeSafeMatcher<Element>() {
      @Override
      protected boolean matchesSafely(Element item) {
        return item.getPayload().equals(buffer(i));
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("element containing buffer[" + i +"]");
      }
    };
  }

  private void emptyAndValidate(OffHeapChainMap<String> map) {
    for (String key : map.keySet()) {
      map.replaceAtHead(key, map.get(key), chainOf());
    }
    assertThat(map.getSize(), is(0L));
    assertThat(map.getDataOccupiedMemory(), is(0L));
  }
}
