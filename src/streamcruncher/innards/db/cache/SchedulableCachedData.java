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
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import streamcruncher.innards.db.cache.ObservableCacheConfig.CacheConfigChangeListener;

/*
 * Author: Ashwin Jayaprakash Date: Jul 14, 2007 Time: 8:20:03 PM
 */

public class SchedulableCachedData extends RefreshableCachedData implements
        CacheConfigChangeListener {
    private static final long serialVersionUID = 1L;

    protected final AtomicBoolean forceRefreshLock;

    protected transient RefreshMaster refreshMaster;

    protected transient volatile CacheRefresherJob refresherJob;

    protected transient volatile long lastRefreshTime;

    protected transient volatile long refreshIntervalMsecs;

    public SchedulableCachedData(String sql) {
        this(sql, null);
    }

    public SchedulableCachedData(String sql, ObservableCacheConfig cacheConfig) {
        super(sql, cacheConfig);

        this.forceRefreshLock = new AtomicBoolean();
    }

    @Override
    protected Object readResolve() throws ObjectStreamException {
        return new SchedulableCachedData(this.getSql(), this.getCacheConfig());
    }

    public void init(RefreshMaster master) {
        this.refreshMaster = master;

        cacheRefreshIntervalChanged(0, cacheConfig.getRefreshIntervalMsecs());
    }

    public CacheRefresherJob getRefresherJob() {
        return refresherJob;
    }

    public long getNextRefreshTime() {
        return lastRefreshTime + refreshIntervalMsecs;
    }

    // ------------

    @Override
    protected Object fetchDataForCache() throws SQLException {
        Object obj = super.fetchDataForCache();
        lastRefreshTime = System.currentTimeMillis();
        return obj;
    }

    public boolean attemptForceRefreshLock() {
        return forceRefreshLock.compareAndSet(false, true);
    }

    public boolean isForceRefreshLocked() {
        return forceRefreshLock.get();
    }

    @Override
    public void forceRefresh() throws CacheException {
        /*
         * The Cache was refreshed synchronously, while the Scheduler was
         * asleep.
         */
        if (getNextRefreshTime() > System.currentTimeMillis()) {
            return;
        }

        if (hasListenerQueries() == false) {
            lastRefreshTime = System.currentTimeMillis();
            return;
        }

        super.forceRefresh();
    }

    public void forceRefreshUnlock() {
        forceRefreshLock.set(false);
    }

    public void cacheRefreshIntervalChanged(long oldRefreshIntervalMsecs,
            long newRefreshIntervalMsecs) {
        refreshIntervalMsecs = newRefreshIntervalMsecs;

        if (attemptForceRefreshLock()) {
            if (refresherJob != null) {
                refresherJob.disconnectFromCD();
            }

            refresherJob = new CacheRefresherJob(refreshMaster, this);

            long nextRefreshAt = getNextRefreshTime();
            refresherJob.setScheduleTimeMillis(nextRefreshAt);

            refreshMaster.scheduleForRefresh(this);
        }
    }
}
