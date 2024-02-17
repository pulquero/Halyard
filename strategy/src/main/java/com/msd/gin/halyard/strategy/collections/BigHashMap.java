/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
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
package com.msd.gin.halyard.strategy.collections;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.mapdb.DB;
import org.mapdb.DB.HTreeMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

/**
 * This is a MapDB implementation.
 * Thread-safe.
 * @param <K> Serializable key type
 * @param <V> Serializable value type
 */
public class BigHashMap<K extends Serializable, V extends Serializable> implements Closeable {

    private static final String MAP_NAME = "temp";

    private final AtomicLong counter = new AtomicLong();
    private final AtomicInteger refCounter = new AtomicInteger();
    private final int memoryThreshold;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private volatile Pair<Map<K,V>,DB> mapDb;

    public static <K extends Serializable, V extends Serializable> BigHashMap<K, V> create(int memoryThreshold) {
    	return new BigHashMap<>(memoryThreshold, null, null);
    }

    private BigHashMap(int memoryThreshold, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    	this.memoryThreshold = memoryThreshold;
    	this.keySerializer = keySerializer;
    	this.valueSerializer = valueSerializer;
    	this.mapDb = Pair.of(new ConcurrentHashMap<>(1024), null);
    }

    public V put(K k, V v) throws IOException {
    	Pair<Map<K,V>,DB> localRef = mapDb;
    	if (localRef == null) {
    		throw new IOException("Already closed");
    	}

    	if (shouldSwap(localRef)) {
    		synchronized (this) {
    			localRef = mapDb;
    			if (shouldSwap(localRef)) {
    				// spin lock
    				while (refCounter.get() > 0) {
    					;
    				}
    				localRef = transferToDisk(localRef);
    				mapDb = localRef;
    			}
    		}
    	}

    	V oldValue;
    	refCounter.incrementAndGet();
    	localRef = mapDb;
    	try {
            oldValue = localRef.getLeft().put(k, v);
        } catch (IllegalAccessError err) {
            throw new IOException(err);
        } finally {
        	refCounter.decrementAndGet();
        }
    	if (oldValue == null) {
    		counter.incrementAndGet();
    	}
    	return oldValue;
    }

    private boolean shouldSwap(Pair<Map<K,V>,DB> ref) {
    	return ref.getRight() == null && size() >= memoryThreshold;
    }

    private Pair<Map<K,V>,DB> transferToDisk(Pair<Map<K,V>,DB> ref) {
    	if (ref.getRight() != null) {
    		throw new IllegalStateException();
    	}
    	DB db = DBMaker.newTempFileDB().deleteFilesAfterClose().closeOnJvmShutdown().mmapFileEnableIfSupported().transactionDisable().asyncWriteEnable().make();
    	HTreeMapMaker mapMaker = db.createHashMap(MAP_NAME);
    	if (keySerializer != null) {
    		mapMaker = mapMaker.keySerializer(keySerializer);
    	}
    	if (valueSerializer != null) {
    		mapMaker = mapMaker.valueSerializer(valueSerializer);
    	}
        Map<K, V> map = mapMaker.make();
        map.putAll(ref.getLeft());
        return Pair.of(map, db);
    }

    public Iterable<Map.Entry<K,V>> entrySet() {
    	Pair<Map<K,V>,DB> localRef = mapDb;
    	return localRef.getLeft().entrySet();
    }

    public long size() {
    	return counter.get();
    }

    @Override
    public synchronized void close() {
    	Pair<Map<K,V>,DB> localRef = mapDb;
    	if (localRef != null) {
	        try {
	    		DB db = localRef.getRight();
	    		if (db != null) {
	    			db.close();
	    		}
	        } catch (IllegalAccessError|IllegalStateException ignore) {
	            //silent close
	        } finally {
	        	mapDb = null;
	        }
    	}
    }
}
