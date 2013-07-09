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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import streamcruncher.util.DaemonThreadFactory;
import streamcruncher.util.RunnableDelayQueue;

/*
 * Author: Ashwin Jayaprakash Date: Jul 14, 2007 Time: 6:10:54 PM
 */

public class RefreshMaster {
    protected final ConcurrentMap<String, SchedulableCachedData> configuredCaches;

    protected final RunnableDelayQueue refreshJobQueue;

    protected final ThreadPoolExecutor refreshJobThreadPool;

    public RefreshMaster(ConcurrentMap<String, SchedulableCachedData> configuredCaches,
            int numThreads) {
        this.configuredCaches = configuredCaches;
        this.refreshJobQueue = new RunnableDelayQueue();

        ThreadFactory threadFactory = new DaemonThreadFactory(CacheRefresherJob.class
                .getSimpleName()
                + "-Thread-", Thread.MAX_PRIORITY);

        /*
         * No timeout for Pool and pre-start one Thread, otherwise the Pool runs
         * the submitted job by spawning a new Thread and bypassing the
         * BlockingQueue (no Delay), directly.
         */
        this.refreshJobThreadPool = new ThreadPoolExecutor(1, numThreads, 0, TimeUnit.SECONDS,
                refreshJobQueue, threadFactory);
        this.refreshJobThreadPool.prestartCoreThread();
    }

    public void stop() {
        refreshJobQueue.clear();
        refreshJobThreadPool.shutdown();
    }

    // ------------

    /**
     * {@link SchedulableCachedData#isForceRefreshLocked()} must return true for
     * this method to work.
     * 
     * @param cachedData
     */
    public void scheduleForRefresh(SchedulableCachedData cachedData) {
        if (cachedData.isForceRefreshLocked()) {
            CacheRefresherJob job = cachedData.getRefresherJob();
            cachedData.forceRefreshUnlock();

            // Check if somebody has not already unregistered this CachedData.
            if (configuredCaches.get(cachedData.getSql()) == cachedData) {
                refreshJobThreadPool.execute(job);
            }
        }
    }
}
