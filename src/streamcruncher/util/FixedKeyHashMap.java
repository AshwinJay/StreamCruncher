/*
 * StreamCruncher:  Copyright (c) 2006-2008, Ashwin Jayaprakash. All Rights Reserved.
 * Contact:         ashwin {dot} jayaprakash {at} gmail {dot} com
 * Web:             http://www.StreamCruncher.com
 * 
 * This file is part of StreamCruncher.
 * 
 *     StreamCruncher is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 * 
 *     StreamCruncher is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 * 
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with StreamCruncher. If not, see <http://www.gnu.org/licenses/>.
 */
package streamcruncher.util;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Author: Ashwin Jayaprakash Date: Aug 9, 2006 Time: 4:03:31 PM
 */

/**
 * Similar to an {@link EnumMap}, where the Keys are fixed and can never be
 * removed. The map will <b>always</b> have these keys with the
 * {@link #defaultValue} as the values if nothing was set. <code>null</code>
 * Keys and Values are not allowed. However, the {@link #defaultValue} can be
 * <code>null</code>.
 */
public class FixedKeyHashMap<K, V> extends ConcurrentHashMap<K, V> {
    private static final long serialVersionUID = 1L;

    protected final Set<K> keys;

    protected final V defaultValue;

    public FixedKeyHashMap(Set<K> keys, V defaultValue) {
        this.keys = Collections.unmodifiableSet(keys);
        this.defaultValue = defaultValue;

        init(defaultValue);
    }

    public FixedKeyHashMap(Set<K> keys, V defaultValue, int initialCapacity) {
        super(initialCapacity);

        this.keys = Collections.unmodifiableSet(keys);
        this.defaultValue = defaultValue;

        init(defaultValue);
    }

    public FixedKeyHashMap(Map<K, V> m, V defaultValue) {
        super();

        HashSet<K> serializableSet = new HashSet<K>();
        serializableSet.addAll(m.keySet());
        this.keys = Collections.unmodifiableSet(serializableSet);
        this.defaultValue = defaultValue;

        init(defaultValue);

        putAll(m);
    }

    public FixedKeyHashMap(Set<K> keys, V defaultValue, int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);

        this.keys = Collections.unmodifiableSet(keys);
        this.defaultValue = defaultValue;

        init(defaultValue);
    }

    private void init(V defValue) {
        if (defValue == null) {
            return;
        }

        for (K key : keys) {
            put(key, defValue);
        }
    }

    public V getDefaultValue() {
        return defaultValue;
    }

    /**
     * @return Unmodifiable.
     */
    public Set<K> getKeys() {
        return keys;
    }

    // -------------

    /**
     * Sets the {@link #defaultValue} value for each key.
     */
    @Override
    public void clear() {
        for (K key : keys) {
            remove(key);
        }
    }

    /**
     * @return <code>false</code> always, as the keys are always present.
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * @return Unmodifiable.
     */
    @Override
    public Set<K> keySet() {
        return getKeys();
    }

    /**
     * Sets the value only if the key is valid.
     * 
     * @param key
     * @param value
     */
    @Override
    public V put(K key, V value) {
        if (keys == null /*
                             * doc keys is null only when this is being
                             * deserialized. The parent class here,
                             * unfortunately invokes the put(..) methods which
                             * is over-ridden here. But when this is invoked,
                             * the sub-class i.e this, would not have been
                             * deserialized yet.
                             */
                || keys.contains(key)) {
            return super.put(key, value);
        }

        return null;
    }

    /**
     * Filters and puts key-values pairs of valid Keys.
     * 
     * @param m
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Set<? extends K> newSet = m.keySet();
        for (K k : newSet) {
            if (keys.contains(k)) {
                V v = m.get(k);
                super.put(k, v);
            }
        }
    }

    /**
     * Key will always be present if it is valid.
     * 
     * @param key
     * @param value
     * @return If old-value was <code>null</code>, then new value replaces
     *         old <code>null</code> value.
     */
    @Override
    public V putIfAbsent(K key, V value) {
        V oldVal = get(key);
        if (oldVal == null && keys.contains(key)) {
            put(key, value);
        }

        return oldVal;
    }

    @Override
    public boolean containsKey(Object key) {
        return keys.contains(key);
    }

    /**
     * Sets the value of the key to {@link #defaultValue} only if the key is
     * valid.
     * 
     * @param key
     */
    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        if (keys.contains(key)) {
            if (defaultValue != null) {
                return super.put((K) key, defaultValue);
            }

            return super.remove(key);
        }

        return null;
    }
}
