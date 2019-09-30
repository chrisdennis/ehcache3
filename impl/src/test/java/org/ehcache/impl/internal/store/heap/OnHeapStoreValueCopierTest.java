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

package org.ehcache.impl.internal.store.heap;

import org.ehcache.Cache;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.copy.Copier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.test.MockitoUtil.mock;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

/**
 * OnHeapStoreValueCopierTest
 */
public class OnHeapStoreValueCopierTest {

  @ParameterizedTest(name = "copyForRead: {0} - copyForWrite: {1}")
  @ArgumentsSource(OnHeapStoreKeyCopierTest.Params.class)
  @interface TestAllCopierSettings {}

  static class Params implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
        arguments(false, false),
        arguments(false, true),
        arguments(true, false),
        arguments(true, true)
      );
    }
  }

  private static final Long KEY = 42L;
  private static final Value VALUE = new Value("TheAnswer");
  private static final Supplier<Boolean> NOT_REPLACE_EQUAL = () -> false;
  private static final Supplier<Boolean> REPLACE_EQUAL = () -> true;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @BeforeEach
  public OnHeapStore<Long, Value> createStore(boolean copyForRead, boolean copyForWrite) {
    Store.Configuration<Long, Value> configuration = mock(Store.Configuration.class);
    when(configuration.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build());
    when(configuration.getKeyType()).thenReturn(Long.class);
    when(configuration.getValueType()).thenReturn(Value.class);

    ExpiryPolicy expiryPolicy = ExpiryPolicyBuilder.noExpiration();
    when(configuration.getExpiry()).thenReturn(expiryPolicy);

    Copier<Value> valueCopier = new Copier<Value>() {
      @Override
      public Value copyForRead(Value obj) {
        if (copyForRead) {
          return new Value(obj.state);
        }
        return obj;
      }

      @Override
      public Value copyForWrite(Value obj) {
        if (copyForWrite) {
          return new Value(obj.state);
        }
        return obj;
      }
    };

    return new OnHeapStore<>(configuration, SystemTimeSource.INSTANCE, new IdentityCopier<>(), valueCopier, new NoopSizeOfEngine(),
      NullStoreEventDispatcher.<Long, Value>nullStoreEventDispatcher(), new DefaultStatisticsService());
  }

  @TestAllCopierSettings
  public void testPutAndGet(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    store.put(KEY, VALUE);

    Store.ValueHolder<Value> firstStoreValue = store.get(KEY);
    Store.ValueHolder<Value> secondStoreValue = store.get(KEY);
    compareValues(copyForRead, copyForWrite, VALUE, firstStoreValue.get());
    compareValues(copyForRead, copyForWrite, VALUE, secondStoreValue.get());
    compareReadValues(copyForRead, firstStoreValue.get(), secondStoreValue.get());
  }

  @TestAllCopierSettings
  public void testGetAndCompute(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    store.put(KEY, VALUE);
    Store.ValueHolder<Value> computedVal = store.getAndCompute(KEY, (aLong, value) -> VALUE);
    Store.ValueHolder<Value> oldValue = store.get(KEY);
    store.getAndCompute(KEY, (aLong, value) -> {
      compareReadValues(copyForRead, value, oldValue.get());
      return value;
    });

    compareValues(copyForRead, copyForWrite, VALUE, computedVal.get());
  }

  @TestAllCopierSettings
  public void testComputeWithoutReplaceEqual(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    final Store.ValueHolder<Value> firstValue = store.computeAndGet(KEY, (aLong, value) -> VALUE, NOT_REPLACE_EQUAL, () -> false);
    store.computeAndGet(KEY, (aLong, value) -> {
      compareReadValues(copyForRead, value, firstValue.get());
      return value;
    }, NOT_REPLACE_EQUAL, () -> false);

    compareValues(copyForRead, copyForWrite, VALUE, firstValue.get());
  }

  @TestAllCopierSettings
  public void testComputeWithReplaceEqual(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    final Store.ValueHolder<Value> firstValue = store.computeAndGet(KEY, (aLong, value) -> VALUE, REPLACE_EQUAL, () -> false);
    store.computeAndGet(KEY, (aLong, value) -> {
      compareReadValues(copyForRead, value, firstValue.get());
      return value;
    }, REPLACE_EQUAL, () -> false);

    compareValues(copyForRead, copyForWrite, VALUE, firstValue.get());
  }

  @TestAllCopierSettings
  public void testComputeIfAbsent(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    Store.ValueHolder<Value> computedValue = store.computeIfAbsent(KEY, aLong -> VALUE);
    Store.ValueHolder<Value> secondComputedValue = store.computeIfAbsent(KEY, aLong -> {
      fail("There should have been a mapping");
      return null;
    });
    compareValues(copyForRead, copyForWrite, VALUE, computedValue.get());
    compareReadValues(copyForRead, computedValue.get(), secondComputedValue.get());
  }

  @TestAllCopierSettings
  public void testBulkCompute(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    final Map<Long, Store.ValueHolder<Value>> results = store.bulkCompute(singleton(KEY), entries -> singletonMap(KEY, VALUE).entrySet());
    store.bulkCompute(singleton(KEY), entries -> {
      compareReadValues(copyForRead, results.get(KEY).get(), entries.iterator().next().getValue());
      return entries;
    });
    compareValues(copyForRead, copyForWrite, VALUE, results.get(KEY).get());
  }

  @TestAllCopierSettings
  public void testBulkComputeWithoutReplaceEqual(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    final Map<Long, Store.ValueHolder<Value>> results = store.bulkCompute(singleton(KEY), entries -> singletonMap(KEY, VALUE).entrySet(), NOT_REPLACE_EQUAL);
    store.bulkCompute(singleton(KEY), entries -> {
      compareReadValues(copyForRead, results.get(KEY).get(), entries.iterator().next().getValue());
      return entries;
    }, NOT_REPLACE_EQUAL);
    compareValues(copyForRead, copyForWrite, VALUE, results.get(KEY).get());
  }

  @TestAllCopierSettings
  public void testBulkComputeWithReplaceEqual(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    final Map<Long, Store.ValueHolder<Value>> results = store.bulkCompute(singleton(KEY), entries -> singletonMap(KEY, VALUE).entrySet(), REPLACE_EQUAL);
    store.bulkCompute(singleton(KEY), entries -> {
      compareReadValues(copyForRead, results.get(KEY).get(), entries.iterator().next().getValue());
      return entries;
    }, REPLACE_EQUAL);
    compareValues(copyForRead, copyForWrite, VALUE, results.get(KEY).get());
  }

  @TestAllCopierSettings
  public void testBulkComputeIfAbsent(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    Map<Long, Store.ValueHolder<Value>> results = store.bulkComputeIfAbsent(singleton(KEY), longs -> singletonMap(KEY, VALUE).entrySet());
    Map<Long, Store.ValueHolder<Value>> secondResults = store.bulkComputeIfAbsent(singleton(KEY), longs -> {
      fail("There should have been a mapping!");
      return null;
    });
    compareValues(copyForRead, copyForWrite, VALUE, results.get(KEY).get());
    compareReadValues(copyForRead, results.get(KEY).get(), secondResults.get(KEY).get());
  }

  @TestAllCopierSettings
  public void testIterator(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    OnHeapStore<Long, Value> store = createStore(copyForRead, copyForWrite);
    store.put(KEY, VALUE);
    Store.Iterator<Cache.Entry<Long, Store.ValueHolder<Value>>> iterator = store.iterator();
    assertThat(iterator.hasNext(), is(true));
    while (iterator.hasNext()) {
      Cache.Entry<Long, Store.ValueHolder<Value>> entry = iterator.next();
      compareValues(copyForRead, copyForWrite, entry.getValue().get(), VALUE);
    }
  }

  private void compareValues(boolean copyForRead, boolean copyForWrite, Value first, Value second) {
    if (copyForRead || copyForWrite) {
      assertThat(first, not(sameInstance(second)));
    } else {
      assertThat(first, sameInstance(second));
    }
  }

  private void compareReadValues(boolean copyForRead, Value first, Value second) {
    if (copyForRead) {
      assertThat(first, not(sameInstance(second)));
    } else {
      assertThat(first, sameInstance(second));
    }
  }

  public static final class Value {
    String state;

    public Value(String state) {
      this.state = state;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Value value = (Value) o;
      return state.equals(value.state);
    }

    @Override
    public int hashCode() {
      return state.hashCode();
    }
  }
}
