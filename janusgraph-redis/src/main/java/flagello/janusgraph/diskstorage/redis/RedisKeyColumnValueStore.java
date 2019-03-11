// Copyright 2018 William Esz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flagello.janusgraph.diskstorage.redis;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.lang.String;
import java.util.function.Consumer;

public class RedisKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory.getLogger(RedisKeyColumnValueStore.class);

//    private final RedisCommands<StaticBuffer, StaticBuffer> sync;
//    private final RedisAsyncCommands<StaticBuffer, StaticBuffer> async;
    private final String name;
    private final RedisStoreManager manager;
    private final GenericObjectPool<StatefulRedisConnection<StaticBuffer, StaticBuffer>> pool;

    RedisKeyColumnValueStore(String name, GenericObjectPool<StatefulRedisConnection<StaticBuffer, StaticBuffer>> pool, RedisStoreManager manager) {
        this.pool = pool;
//        this.sync = sync;
//        this.async = sync.getStatefulConnection().async();
        this.name = name;
        this.manager = manager;
    }

    public void add(StaticBuffer key, StaticBuffer column, StaticBuffer value) throws BackendException {
        RedisCommands<StaticBuffer, StaticBuffer> sync=null;
        StatefulRedisConnection<StaticBuffer, StaticBuffer> connection = null;
        try {
            connection = pool.borrowObject();
//            log.info("Add to redis, pool hash is {},pool size is {},connection object {}",pool.hashCode(),pool.listAllObjects().size(),connection);
            sync = connection.sync();
            sync.multi();
            sync.hset(key, column, value); // Key-indexed Hash
            sync.sadd(column, key); // Column-indexed Set
            sync.sadd(manager.KEYS_SET_KEY, key); // All keys
            sync.sadd(manager.COLUMNS_SET_KEY, column); // All columns
            sync.exec();
        } catch (Exception e) {
            e.printStackTrace();
            if(sync!=null){
                sync.discard();
            }
            throw new PermanentBackendException(e);
        }finally {
            if(connection!=null) pool.returnObject(connection);
        }
    }

    public void del(StaticBuffer key, StaticBuffer column) throws BackendException {
        RedisCommands<StaticBuffer, StaticBuffer> sync=null;
        StatefulRedisConnection<StaticBuffer, StaticBuffer> connection = null;
        try{
            connection = pool.borrowObject();
            sync = connection.sync();
            sync.multi();
            sync.hdel(key, column);
            sync.srem(column, key);
            sync.exec();

            cleanIndexes(key, column);
        } catch (Exception e) {
            e.printStackTrace();
            if(sync!=null){
                sync.discard();
            }
            throw new PermanentBackendException(e);
        }finally {
            if(connection!=null) pool.returnObject(connection);
        }
    }

    private void cleanIndexes(StaticBuffer key, StaticBuffer column) {
        StatefulRedisConnection<StaticBuffer, StaticBuffer> connection=null;
        try{
            connection = pool.borrowObject();
            RedisAsyncCommands<StaticBuffer, StaticBuffer> async = connection.async();
            async.exists(key).thenAcceptAsync(exists -> {
                if (exists == 0) {
                    async.srem(manager.KEYS_SET_KEY, key);
                }
            });

            async.scard(column).thenAcceptAsync(cardinality -> {
                if (cardinality == 0) {
                    async.srem(manager.COLUMNS_SET_KEY, column);
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(connection!=null) pool.returnObject(connection);
        }
    }

    @Override // This method is only supported by stores which keep keys in byte-order.
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        final StaticBuffer keyStart = query.getKeyStart();
        final StaticBuffer keyEnd = query.getKeyEnd();
        final StaticBuffer columnStart = query.getSliceStart();
        final StaticBuffer columnEnd = query.getSliceEnd();

        // final int limit = query.getLimit();
        //
        // `limit` refers to the column count rather than the key count.
        // See if limit is intended as an upper bound?

        final Map<StaticBuffer, Set<StaticBuffer>> results = new HashMap<>();
        StatefulRedisConnection<StaticBuffer, StaticBuffer> connection = null;
        try{
            connection = pool.borrowObject();
            RedisCommands<StaticBuffer, StaticBuffer> sync = connection.sync();
            // 1. Start from columns as they are the inner filter.
            ScanIterator.sscan(sync, manager.COLUMNS_SET_KEY)
                .forEachRemaining(column -> {
                    // 2. Apply the query column filter.
                    if (matches(columnStart, columnEnd, column)) {
                        // 3. Find keys which have the given column.
                        ScanIterator.sscan(sync, column)
                            .forEachRemaining(key -> {
                                // 4. Apply the query key filter.
                                if (matches(keyStart, keyEnd, key)) {
                                    results.putIfAbsent(key, new HashSet<>());
                                    results.get(key).add(column);
                                }
                            });
                    }
                });
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }finally {
            if(connection!=null) pool.returnObject(connection);
        }

        return keyIteratorFactory(results);
    }

    @Override // This method is only supported by stores which do not keep keys in byte-order.
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        final StaticBuffer columnStart = query.getSliceStart();
        final StaticBuffer columnEnd = query.getSliceEnd();
        // final int limit = query.getLimit();

        final Map<StaticBuffer, Set<StaticBuffer>> results = new HashMap<>();
        StatefulRedisConnection<StaticBuffer, StaticBuffer> connection = null;
        try {
            connection = pool.borrowObject();
            RedisCommands<StaticBuffer, StaticBuffer> sync = connection.sync();
            // 1. Scan the columns set.
            ScanIterator.sscan(sync, manager.COLUMNS_SET_KEY)
                .forEachRemaining(column -> {
                    // 2. Apply the query filter.
                    if (matches(columnStart, columnEnd, column)) {
                        // 3. Find the keys which have the given column.
                        ScanIterator.sscan(sync, column)
                            .forEachRemaining(key -> {
                                results.putIfAbsent(key, new HashSet<>());
                                results.get(key).add(column);
                            });
                    }
                });
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }finally {
            if(connection!=null) pool.returnObject(connection);
        }

        return keyIteratorFactory(results);
    }

    private KeyIterator keyIteratorFactory(Map<StaticBuffer, Set<StaticBuffer>> results) {
        return new KeyIterator() {
            private Iterator<StaticBuffer> keyIterator = results.keySet().iterator();
            private StaticBuffer currentKey;

            @Override
            public boolean hasNext() { return keyIterator.hasNext(); }

            @Override
            public StaticBuffer next() {
                currentKey = keyIterator.next();
                return currentKey;
            }

            @Override
            public RecordIterator<Entry> getEntries() {
                return new RecordIterator<Entry>() {
                    private Iterator<StaticBuffer> columnIterator = results.get(currentKey).iterator();

                    @Override
                    public boolean hasNext() {
                        return columnIterator.hasNext();
                    }

                    @Override
                    public Entry next() {
                        final StaticBuffer column = columnIterator.next();
                        StaticBuffer value = null;
                        StatefulRedisConnection<StaticBuffer, StaticBuffer> connection = null;
                        try {
                            connection = pool.borrowObject();
                            value = connection.sync().hget(currentKey, column);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }finally {
                            if(connection!=null) pool.returnObject(connection);
                        }
                        return StaticArrayEntry.of(column, value);
                    }

                    @Override
                    public void close() { }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void close() { }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        final StaticBuffer key = query.getKey();
        final StaticBuffer columnStart = query.getSliceStart();
        final StaticBuffer columnEnd = query.getSliceEnd();
        // final int limit = query.getLimit();

        final EntryArrayList result = new EntryArrayList();
        StatefulRedisConnection<StaticBuffer, StaticBuffer> connection = null;
        try{
            connection = pool.borrowObject();
            RedisCommands<StaticBuffer, StaticBuffer> sync = connection.sync();
            ScanIterator.hscan(sync, key)
                .forEachRemaining(columnValue -> {
                    final StaticBuffer column = columnValue.getKey();
                    if (RedisKeyColumnValueStore.this.matches(columnStart, columnEnd, column)) {
                        result.add(StaticArrayEntry.of(column, columnValue.getValue()));
                    }
                });
        } catch (Exception e) {
            e.printStackTrace();
            log.info("RedisException: "+e.getCause());
            throw new PermanentBackendException(e);
        }finally {
            if(connection!=null) pool.returnObject(connection);
        }

        return result;
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        final Map<StaticBuffer, EntryList> result = new HashMap<>();

        for (StaticBuffer key : keys)
            result.put(key, getSlice(new KeySliceQuery(key, query), txh));

        return result;
    }

    private Boolean matches(StaticBuffer start, StaticBuffer end, StaticBuffer item) {
        return item.compareTo(start) >= 0 && !(item.compareTo(end) >= 0);  // `start` is inclusive; `end` is exclusive
    } // See package org.janusgraph.diskstorage.keycolumnvalue.KCVUtil

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        mutateOneKey(key, new KCVMutation(additions, deletions), txh);
    }

    private void mutateOneKey(final StaticBuffer key, final KCVMutation mutation, final StoreTransaction txh) throws BackendException {
        manager.mutateMany(Collections.singletonMap(name, Collections.singletonMap(key, mutation)), txh);
    }

    @Override
    public void acquireLock(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue, final StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void close() throws BackendException {
//        pool.close();
//        sync.getStatefulConnection().close();
        manager.removeDatabase(this);
    }

    public synchronized void clear() {
        StatefulRedisConnection<StaticBuffer, StaticBuffer> connection = null;
        try {
            connection = pool.borrowObject();
            RedisCommands<StaticBuffer, StaticBuffer> sync = connection.sync();
            sync.flushdb();
            // https://redis.io/commands/flushdb
            // This command never fails and is asynchronous.
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(connection!=null) pool.returnObject(connection);
        }
    }

    @Override
    public String getName() {
        return name;
    }
}
