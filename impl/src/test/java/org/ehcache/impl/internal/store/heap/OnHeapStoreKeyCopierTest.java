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
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * OnHeapStoreKeyCopierTest
 */
public class OnHeapStoreKeyCopierTest {

  @ParameterizedTest(name = "copyForRead: {0} - copyForWrite: {1}")
  @ArgumentsSource(Params.class)
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
  private static final Key KEY = new Key("WHat?");
  private static final String VALUE = "TheAnswer";
  private static final Supplier<Boolean> NOT_REPLACE_EQUAL = () -> false;
  private static final Supplier<Boolean> REPLACE_EQUAL = () -> true;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public Store<Key, String> createStore(boolean copyForRead, boolean copyForWrite) {
    Store.Configuration<Key, String> configuration = mock(Store.Configuration.class);
    when(configuration.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build());
    when(configuration.getKeyType()).thenReturn(Key.class);
    when(configuration.getValueType()).thenReturn(String.class);
    when(configuration.getExpiry()).thenReturn((ExpiryPolicy) ExpiryPolicyBuilder.noExpiration());

    Copier<Key> keyCopier = new Copier<Key>() {
      @Override
      public Key copyForRead(Key obj) {
        if (copyForRead) {
          return new Key(obj);
        }
        return obj;
      }

      @Override
      public Key copyForWrite(Key obj) {
        if (copyForWrite) {
          return new Key(obj);
        }
        return obj;
      }
    };

    return new OnHeapStore<>(configuration, SystemTimeSource.INSTANCE, keyCopier, IdentityCopier.identityCopier(),
      new NoopSizeOfEngine(), NullStoreEventDispatcher.nullStoreEventDispatcher(), new DefaultStatisticsService());
  }

  @TestAllCopierSettings
  public void testPutAndGet(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    Key copyKey = new Key(KEY);
    store.put(copyKey, VALUE);

    copyKey.state = "Different!";

    Store.ValueHolder<String> firstStoreValue = store.get(KEY);
    Store.ValueHolder<String> secondStoreValue = store.get(copyKey);
    if (copyForWrite) {
      assertThat(firstStoreValue.get(), is(VALUE));
      assertThat(secondStoreValue, nullValue());
    } else {
      assertThat(firstStoreValue, nullValue());
      assertThat(secondStoreValue.get(), is(VALUE));
    }
  }

  @TestAllCopierSettings
  public void testCompute(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    final Key copyKey = new Key(KEY);
    store.getAndCompute(copyKey, (key, value) -> {
      assertThat(key, is(copyKey));
      return VALUE;
    });
    copyKey.state = "Different!";
    store.getAndCompute(copyKey, (key, value) -> {
      if (copyForWrite) {
        assertThat(value, nullValue());
      } else {
        assertThat(value, is(VALUE));
        assertThat(key, is(copyKey));
        if (copyForRead) {
          key.state = "Changed!";
        }
      }
      return value;
    });

    if (copyForRead) {
      assertThat(copyKey.state, is("Different!"));
    }
  }

  @TestAllCopierSettings
  public void testComputeWithoutReplaceEqual(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    final Key copyKey = new Key(KEY);
    store.computeAndGet(copyKey, (key, value) -> {
      assertThat(key, is(copyKey));
      return VALUE;
    }, NOT_REPLACE_EQUAL, () -> false);
    copyKey.state = "Different!";
    store.computeAndGet(copyKey, (key, value) -> {
      if (copyForWrite) {
        assertThat(value, nullValue());
      } else {
        assertThat(value, is(VALUE));
        assertThat(key, is(copyKey));
        if (copyForRead) {
          key.state = "Changed!";
        }
      }
      return value;
    }, NOT_REPLACE_EQUAL, () -> false);

    if (copyForRead) {
      assertThat(copyKey.state, is("Different!"));
    }
  }

  @TestAllCopierSettings
  public void testComputeWithReplaceEqual(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    final Key copyKey = new Key(KEY);
    store.computeAndGet(copyKey, (key, value) -> {
      assertThat(key, is(copyKey));
      return VALUE;
    }, REPLACE_EQUAL, () -> false);
    copyKey.state = "Different!";
    store.computeAndGet(copyKey, (key, value) -> {
      if (copyForWrite) {
        assertThat(value, nullValue());
      } else {
        assertThat(value, is(VALUE));
        assertThat(key, is(copyKey));
        if (copyForRead) {
          key.state = "Changed!";
        }
      }
      return value;
    }, REPLACE_EQUAL, () -> false);

    if (copyForRead) {
      assertThat(copyKey.state, is("Different!"));
    }
  }

  @TestAllCopierSettings
  public void testIteration(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    store.put(KEY, VALUE);
    Store.Iterator<Cache.Entry<Key, Store.ValueHolder<String>>> iterator = store.iterator();
    assertThat(iterator.hasNext(), is(true));
    while (iterator.hasNext()) {
      Cache.Entry<Key, Store.ValueHolder<String>> entry = iterator.next();
      if (copyForRead || copyForWrite) {
        assertThat(entry.getKey(), not(sameInstance(KEY)));
      } else {
        assertThat(entry.getKey(), sameInstance(KEY));
      }
    }
  }

  @TestAllCopierSettings
  public void testComputeIfAbsent(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    store.computeIfAbsent(KEY, key -> {
      if (copyForRead || copyForWrite) {
        assertThat(key, not(sameInstance(KEY)));
      } else {
        assertThat(key, sameInstance(KEY));
      }
      return VALUE;
    });
  }

  @TestAllCopierSettings
  public void testBulkCompute(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    final AtomicReference<Key> keyRef = new AtomicReference<>();
    store.bulkCompute(singleton(KEY), entries -> {
      Key key = entries.iterator().next().getKey();
      if (copyForRead || copyForWrite) {
        assertThat(key, not(sameInstance(KEY)));
      } else {
        assertThat(key, sameInstance(KEY));
      }
      keyRef.set(key);
      return singletonMap(KEY, VALUE).entrySet();
    });

    store.bulkCompute(singleton(KEY), entries -> {
      if (copyForRead) {
        assertThat(entries.iterator().next().getKey(), not(sameInstance(keyRef.get())));
      }
      return singletonMap(KEY, VALUE).entrySet();
    });
  }

  @TestAllCopierSettings
  public void testBulkComputeWithoutReplaceEqual(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    store.bulkCompute(singleton(KEY), entries -> {
      if (copyForRead || copyForWrite) {
        assertThat(entries.iterator().next().getKey(), not(sameInstance(KEY)));
      } else {
        assertThat(entries.iterator().next().getKey(), sameInstance(KEY));
      }
      return singletonMap(KEY, VALUE).entrySet();
    }, NOT_REPLACE_EQUAL);
  }

  @TestAllCopierSettings
  public void testBulkComputeWithReplaceEqual(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    store.bulkCompute(singleton(KEY), entries -> {
      if (copyForRead || copyForWrite) {
        assertThat(entries.iterator().next().getKey(), not(sameInstance(KEY)));
      } else {
        assertThat(entries.iterator().next().getKey(), sameInstance(KEY));
      }
      return singletonMap(KEY, VALUE).entrySet();
    }, REPLACE_EQUAL);
  }

  @TestAllCopierSettings
  public void testBulkComputeIfAbsent(boolean copyForRead, boolean copyForWrite) throws StoreAccessException {
    Store<Key, String> store = createStore(copyForRead, copyForWrite);
    store.bulkComputeIfAbsent(singleton(KEY), keys -> {
      if (copyForWrite || copyForRead) {
        assertThat(keys.iterator().next(), not(sameInstance(KEY)));
      } else {
        assertThat(keys.iterator().next(), sameInstance(KEY));
      }
      return singletonMap(KEY, VALUE).entrySet();
    });
  }

  public static final class Key {
    String state;
    final int hashCode;

    public Key(String state) {
      this.state = state;
      this.hashCode = state.hashCode();
    }

    public Key(Key key) {
      this.state = key.state;
      this.hashCode = key.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Key value = (Key) o;
      return state.equals(value.state);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
