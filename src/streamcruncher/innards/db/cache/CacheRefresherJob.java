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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Registry;
import streamcruncher.util.DelayedRunnable;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.sysevent.SystemEvent;
import streamcruncher.util.sysevent.SystemEventBus;
import streamcruncher.util.sysevent.SystemEvent.Priority;

/*
 * Author: Ashwin Jayaprakash Date: Aug 13, 2006 Time: 7:07:46 PM
 */

public class CacheRefresherJob implements DelayedRunnable {
    protected final RefreshMaster refreshMaster;

    protected volatile SchedulableCachedData cachedData;

    protected long scheduleTimeMillis;

    public CacheRefresherJob(RefreshMaster refreshMaster, SchedulableCachedData cachedData) {
        this.refreshMaster = refreshMaster;
        this.cachedData = cachedData;
    }

    public void disconnectFromCD() {
        cachedData = null;
    }

    // -------------

    public void setScheduleTimeMillis(long scheduleTimeMillis) {
        this.scheduleTimeMillis = scheduleTimeMillis;
    }

    public long getScheduleTimeMillis() {
        return scheduleTimeMillis;
    }

    // -------------

    public void run() {
        final SchedulableCachedData localCachedDataRef = cachedData;

        if (
        /* This is an abandoned Job due to forced rescheduling */
        localCachedDataRef == null ||
        /* Attempt to lock CD */
        localCachedDataRef.attemptForceRefreshLock() == false) {
            return;
        }

        // -------------

        try {
            localCachedDataRef.forceRefresh();
            scheduleTimeMillis = localCachedDataRef.getNextRefreshTime();
        }
        catch (Throwable t) {
            Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                    CacheRefresherJob.class.getName());
            logger.log(Level.SEVERE, "An error occurred while scheduling the Cache for: "
                    + localCachedDataRef.getSql(), t);

            SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
            SystemEvent event = new SystemEvent(CacheRefresherJob.class.getName(),
                    localCachedDataRef.getSql(), t, Priority.SEVERE);
            bus.submit(event);
        }
        finally {
            refreshMaster.scheduleForRefresh(localCachedDataRef);
        }
    }

    // -------------

    public long getDelay(TimeUnit unit) {
        long delay = (scheduleTimeMillis - System.currentTimeMillis());
        return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed that) {
        long d = getDelay(TimeUnit.MILLISECONDS) - that.getDelay(TimeUnit.MILLISECONDS);

        return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
    }
}
