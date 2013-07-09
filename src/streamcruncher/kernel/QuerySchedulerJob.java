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
package streamcruncher.kernel;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Registry;
import streamcruncher.util.DelayedRunnable;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.TimeKeeper;
import streamcruncher.util.sysevent.SystemEvent;
import streamcruncher.util.sysevent.SystemEventBus;
import streamcruncher.util.sysevent.SystemEvent.Priority;

/*
 * Author: Ashwin Jayaprakash Date: Aug 13, 2006 Time: 7:07:46 PM
 */

/**
 * Lightweight shell over {@link PrioritizedSchedulableQuery} for scheduling.
 */
public class QuerySchedulerJob implements DelayedRunnable {
    protected long scheduleTimeMillis;

    protected volatile PrioritizedSchedulableQuery psq;

    protected QueryMaster queryMaster;

    protected QueryConfig queryConfig;

    protected TimeKeeper timeKeeper;

    public QuerySchedulerJob(PrioritizedSchedulableQuery prioritizedSchedulableQuery) {
        this.psq = prioritizedSchedulableQuery;
        this.queryConfig = psq.getQueryConfig();
    }

    public void init() {
        queryMaster = Registry.getImplFor(QueryMaster.class);
        timeKeeper = Registry.getImplFor(TimeKeeper.class);

        scheduleTimeMillis = timeKeeper.getTimeMsecs();
    }

    /**
     * This method should be invoked only if
     * {@link PrioritizedSchedulableQuery#attemptToSetBusy()} succeeds.
     */
    public void disconnectFromPSQ() {
        psq = null;
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
        final PrioritizedSchedulableQuery localPSQRef = psq;

        if (
        /* This is an abandoned Job due to forced rescheduling */
        localPSQRef == null ||
        /* Attempt to lock PSQ */
        localPSQRef.attemptToSetBusy() == false) {
            return;
        }

        // -------------

        boolean returnFlg = true;
        try {
            // Re-schedule until resumed.
            if (queryConfig.isQueryPaused()) {
                long delay = queryConfig.getResumeCheckTimeMsecs();
                long lastRanAt = queryConfig.getQueryLastRanAt();
                scheduleTimeMillis = (lastRanAt > scheduleTimeMillis) ? (lastRanAt + delay)
                        : (scheduleTimeMillis + delay);
            }
            else {
                returnFlg = false;
            }
        }
        finally {
            if (returnFlg) {
                queryMaster.readyForScheduling(localPSQRef);

                return;
            }
        }

        // -------------

        try {
            localPSQRef.beforeCalculateRunPriority();
            localPSQRef.calculateRunPriority();

            int priority = localPSQRef.afterCalculateRunPriority();

            QueryRunnerJob runnerJob = localPSQRef.getQueryRunnerJob();
            runnerJob.setPriority(priority);

            queryMaster.readyForRunning(localPSQRef);
        }
        catch (Throwable t) {
            try {
                Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                        QuerySchedulerJob.class.getName());
                logger.log(Level.SEVERE, "An error occurred while scheduling the Query: "
                        + localPSQRef.getName(), t);

                SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
                SystemEvent event = new SystemEvent(QuerySchedulerJob.class.getName(), localPSQRef
                        .getName(), t, Priority.SEVERE);
                bus.submit(event);

                // -------------

                /*
                 * Re-schedule on error. An error at this stage would not be
                 * very bad, as the Window.makeLatestMonotonicIdVisible()
                 * wouldn't have been invoked yet.
                 */

                queryConfig.incrementQueryErrorCount();
                localPSQRef.beforeCalculateScheduleTime();
                localPSQRef.calculateScheduleTime();

                scheduleTimeMillis = localPSQRef.afterCalculateScheduleTime();
            }
            finally {
                queryMaster.readyForScheduling(localPSQRef);
            }
        }
    }

    // -------------

    public long getDelay(TimeUnit unit) {
        long delay = (scheduleTimeMillis - timeKeeper.getTimeMsecs());
        return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed that) {
        long d = getDelay(TimeUnit.MILLISECONDS) - that.getDelay(TimeUnit.MILLISECONDS);

        return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
    }
}
