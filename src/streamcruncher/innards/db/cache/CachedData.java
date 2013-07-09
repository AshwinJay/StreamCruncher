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
package streamcruncher.innards.db.cache;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;

/*
 * Author: Ashwin Jayaprakash Date: Jul 12, 2007 Time: 7:46:22 PM
 */

public abstract class CachedData implements Serializable {
    protected final String sql;

    protected transient final CopyOnWriteArraySet<String> listenerQueryNames;

    protected transient final CachedDataStats stats;

    protected transient volatile SoftReference<Object> dataHolder;

    public CachedData(String sql) {
        this.sql = sql;
        this.listenerQueryNames = new CopyOnWriteArraySet<String>();
        this.stats = new CachedDataStats();
    }

    /**
     * @return Clean new instance based on the minimum data that was persisted.
     * @throws ObjectStreamException
     */
    protected abstract Object readResolve() throws ObjectStreamException;

    public String getSql() {
        return sql;
    }

    public void addListenerQueryName(String queryName) {
        listenerQueryNames.add(queryName);
    }

    public boolean hasListenerQueries() {
        return !listenerQueryNames.isEmpty();
    }

    public Collection<String> getListenerQueryNames() {
        return listenerQueryNames;
    }

    /**
     * @param queryName
     * @return The number of Listeners remaining;
     */
    public int removeListenerQueryName(String queryName) {
        listenerQueryNames.remove(queryName);
        return listenerQueryNames.size();
    }

    public CachedDataStats getStats() {
        return stats;
    }

    public abstract ObservableCacheConfig getCacheConfig();

    /**
     * <p>
     * Atomic operation. Thread safe.
     * </p>
     * 
     * @return <code>null</code> if the data has been cleared by the GC/VM.
     * @throws CacheException
     */
    public Object getData() throws CacheException {
        return dataHolder == null ? null : dataHolder.get();
    }

    /**
     * <p>
     * Atomic operation. Thread safe.
     * </p>
     * 
     * @param data
     */
    public void setData(Object data) {
        this.dataHolder = new SoftReference<Object>(data);
    }

    public abstract void forceRefresh() throws CacheException;

    public void discard() {
        dataHolder = null;
    }
}
