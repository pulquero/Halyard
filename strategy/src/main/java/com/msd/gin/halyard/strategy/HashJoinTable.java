package com.msd.gin.halyard.strategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;

abstract class HashJoinTable<K> {
	final Set<String> joinKeySet;
	final String[] joinBindings;
	final String[] buildBindings;
	private final Map<K, List<BindingSetValues>> hashTable;
	private int keyCount;
	private int bsCount;

	static HashJoinTable<?> create(int initialSize, Set<String> joinBindings, List<String> buildBindings) {
		if (joinBindings.size() == 1) {
			String joinBinding = joinBindings.iterator().next();
			return new SingleKeyHashJoinTable(initialSize, joinBinding, buildBindings);
		} else {
			return new MultiKeyHashJoinTable(initialSize, joinBindings, buildBindings);
		}
	}

	HashJoinTable(int initialSize, Set<String> joinBindings, List<String> buildBindings) {
		this.joinKeySet = joinBindings;
		this.joinBindings = joinBindings.toArray(new String[joinBindings.size()]);
		this.buildBindings = buildBindings.toArray(new String[buildBindings.size()]);
		if (!joinBindings.isEmpty()) {
			hashTable = new HashMap<>(initialSize);
		} else {
			hashTable = Collections.<K, List<BindingSetValues>>singletonMap(createKey(EmptyBindingSet.getInstance()), new ArrayList<>(initialSize));
		}
	}

	void put(BindingSet bs) {
		K hashKey = createKey(bs);
		List<BindingSetValues> hashValue = hashTable.get(hashKey);
		boolean newEntry = (hashValue == null);
		if (newEntry) {
			int averageSize = (keyCount > 0) ? (int) (bsCount/keyCount) : 0;
			hashValue = new ArrayList<>(averageSize + 1);
			hashTable.put(hashKey, hashValue);
			keyCount++;
		}
		hashValue.add(BindingSetValues.create(buildBindings, bs));
		bsCount++;
	}

	int entryCount() {
		return bsCount;
	}

	List<BindingSetValues> get(BindingSet bs) {
		K key = createKey(bs);
		return hashTable.get(key);
	}

	Collection<? extends List<BindingSetValues>> all() {
		return hashTable.values();
	}

	abstract K createKey(BindingSet bs);

	private static final class MultiKeyHashJoinTable extends HashJoinTable<BindingSetValues> {
		MultiKeyHashJoinTable(int initialSize, Set<String> joinBindings, List<String> buildBindings) {
			super(initialSize, joinBindings, buildBindings);
		}

		@Override
		BindingSetValues createKey(BindingSet bs) {
			return BindingSetValues.create(joinBindings, bs);
		}
	}

	private static final class SingleKeyHashJoinTable extends HashJoinTable<Value> {
		private final String joinBinding;

		SingleKeyHashJoinTable(int initialSize, String joinBinding, List<String> buildBindings) {
			super(initialSize, Collections.singleton(joinBinding), buildBindings);
			this.joinBinding = joinBinding;
		}

		@Override
		Value createKey(BindingSet bs) {
			Value v = bs.getValue(joinBinding);
			return (v != null) ? v : NullValue.INSTANCE;
		}
	}
}