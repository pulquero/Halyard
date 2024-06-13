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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.mapdb.DB;
import org.mapdb.DB.HashSetMaker;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

/**
 * This is a MapDB implementation.
 * Thread-safe.
 * @author Adam Sotona (MSD)
 * @param <E> Serializable element type
 */
public class BigHashSet<E extends Serializable> implements Iterable<E>, Closeable {

    private static final String SET_NAME = "temp";

    private final AtomicLong counter = new AtomicLong();
    private final AtomicInteger refCounter = new AtomicInteger();
    private final int memoryThreshold;
    private final Serializer<E> serializer;
    private volatile Pair<Set<E>,DB> setDb;

    public static <E extends Serializable> BigHashSet<E> create(int memoryThreshold, Serializer<E> serializer) {
    	return new BigHashSet<>(memoryThreshold, serializer);
    }

    public static BigHashSet<Value> createValueSet(int memoryThreshold, ValueFactory vf) {
    	return create(memoryThreshold, new ValueSerializer(vf));
    }

    public static BigHashSet<BindingSet> createBindingSetSet(int memoryThreshold, ValueFactory vf) {
    	return create(memoryThreshold, new BindingSetSerializer(vf));
    }

    public static <E extends Serializable> BigHashSet<E> create(int memoryThreshold) {
    	return create(memoryThreshold, null);
    }

    private BigHashSet(int memoryThreshold, Serializer<E> serializer) {
    	this.memoryThreshold = memoryThreshold;
    	this.serializer = serializer;
    	this.setDb = Pair.of(ConcurrentHashMap.newKeySet(1024), null);
    }

    /**
     * Adds element to the BigHashSet
     * @param e Serializable element
     * @return boolean if the element has been already present
     * @throws IOException throws IOException in case of problem with underlying storage
     */
    public boolean add(E e) throws IOException {
    	Pair<Set<E>,DB> localRef = setDb;
    	if (localRef == null) {
    		throw new IOException("Already closed");
    	}

    	if (shouldSwap(localRef)) {
    		synchronized (this) {
    			localRef = setDb;
    			if (shouldSwap(localRef)) {
    				// spin lock
    				while (refCounter.get() > 0) {
    					;
    				}
    				localRef = transferToDisk(localRef);
    				setDb = localRef;
    			}
    		}
    	}

    	boolean added;
    	refCounter.incrementAndGet();
    	localRef = setDb;
    	try {
            added = localRef.getLeft().add(e);
        } catch (IllegalAccessError err) {
            throw new IOException(err);
        } finally {
        	refCounter.decrementAndGet();
        }
    	if (added) {
    		counter.incrementAndGet();
    	}
    	return added;
    }

    private boolean shouldSwap(Pair<Set<E>,DB> ref) {
    	return ref.getRight() == null && size() >= memoryThreshold;
    }

    private Pair<Set<E>,DB> transferToDisk(Pair<Set<E>,DB> ref) {
    	if (ref.getRight() != null) {
    		throw new IllegalStateException();
    	}
    	DB db = DBMaker.tempFileDB().fileDeleteAfterClose().closeOnJvmShutdown().fileMmapEnableIfSupported().make();
    	HashSetMaker setMaker = db.hashSet(SET_NAME);
    	if (serializer != null) {
    		setMaker = setMaker.serializer(serializer);
    	}
        Set<E> set = (Set<E>) setMaker.create();
        set.addAll(ref.getLeft());
        return Pair.of(set, db);
    }

    @Override
    public Iterator<E> iterator() {
    	Pair<Set<E>,DB> localRef = setDb;
        return localRef.getLeft().iterator();
    }

    /**
     * Checks for element presence in the BigHashSet
     * @param e Serializable element
     * @return boolean if the element has been present
     * @throws IOException throws IOException in case of problem with underlying storage
     */
    public boolean contains(E e) throws IOException {
    	Pair<Set<E>,DB> localRef = setDb;
    	if (localRef == null) {
    		throw new IOException("Already closed");
    	}

    	try {
            return localRef.getLeft().contains(e);
        } catch (IllegalAccessError err) {
            throw new IOException(err);
        }
    }

    public long size() {
    	return counter.get();
    }

    @Override
    public synchronized void close() {
    	Pair<Set<E>,DB> localRef = setDb;
    	if (localRef != null) {
	        try {
	    		DB db = localRef.getRight();
	    		if (db != null) {
	    			db.close();
	    		}
	        } catch (IllegalAccessError|IllegalStateException ignore) {
	            //silent close
	        } finally {
	        	setDb = null;
	        }
    	}
    }


	private static class ValueSerializer extends AbstractValueSerializer<Value> {
    	public ValueSerializer() {
    		// required for deserialization
    	}

    	ValueSerializer(ValueFactory vf) {
    		super(vf);
    	}

    	@Override
		public void serialize(DataOutput2 out, Value value) throws IOException {
			ByteBuffer tmp = newTempBuffer();
			writeValue(value, out, tmp);
		}

		@Override
		public Value deserialize(DataInput2 in, int available) throws IOException {
			return readValue(in);
		}
    }


    private static class BindingSetSerializer extends AbstractValueSerializer<BindingSet> {
		public BindingSetSerializer() {
			// required for deserialization
		}

		BindingSetSerializer(ValueFactory vf) {
			super(vf);
		}

		@Override
		public void serialize(DataOutput2 out, BindingSet bs) throws IOException {
			ByteBuffer tmp = newTempBuffer();
			writeBindingSet(bs, out, tmp);
		}

		@Override
		public BindingSet deserialize(DataInput2 in, int available) throws IOException {
			return readBindingSet(in);
		}
    }
}
