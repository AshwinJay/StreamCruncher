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

import java.io.ObjectStreamException;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import streamcruncher.api.QueryConfig.QuerySchedulePolicy;
import streamcruncher.api.QueryConfig.QuerySchedulePolicyValue;
import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.EventBucket;
import streamcruncher.innards.core.EventBucketClient;
import streamcruncher.innards.core.filter.FilteredTable;

/*
 * Author: Ashwin Jayaprakash Date: Jan 10, 2006 Time: 11:11:28 PM
 */

/**
 * Use {@link #attemptToSetBusy()} and {@link #setNotBusy()} to have exclusive
 * access.
 */
public class PrioritizedSchedulableQuery extends SchedulableQuery implements EventBucketClient {
    private static final long serialVersionUID = 1L;

    public static final long ON_PAUSE_RECHECK_TIME_MILLIS = 10 * 1000;

    // -------------

    protected transient QuerySchedulerJob querySchedulerJob;

    protected transient QueryMaster queryMaster;

    protected transient final QueryRunnerJob queryRunnerJob;

    protected final AtomicBoolean busy;

    protected final AtomicBoolean newEventNotifierLock;

    protected final Lock eventWeightLock;

    // -------------

    protected transient volatile long scheduleAt;

    protected transient volatile int priority;

    protected transient volatile float accumulatedNewEventWeight;

    // -------------

    /**
     * @param runningQuery
     * @param queueHolder
     * @param querySchedulerThreadPool
     * @param timingHistorySize
     * @param maxPenaltyRuns
     * @param completionListener
     * @throws KernelException
     */
    public PrioritizedSchedulableQuery(RunningQuery runningQuery) throws KernelException {
        super(runningQuery);

        this.querySchedulerJob = new QuerySchedulerJob(this);
        this.queryRunnerJob = new QueryRunnerJob(this);

        this.busy = new AtomicBoolean(true);
        this.newEventNotifierLock = new AtomicBoolean(false);
        this.eventWeightLock = new ReentrantLock();
    }

    public PrioritizedSchedulableQuery(SchedulableQuery schedulableQuery) throws KernelException {
        super(schedulableQuery);

        this.querySchedulerJob = new QuerySchedulerJob(this);
        this.queryRunnerJob = new QueryRunnerJob(this);

        this.busy = new AtomicBoolean(true);
        this.newEventNotifierLock = new AtomicBoolean(false);
        this.eventWeightLock = new ReentrantLock();
    }

    @Override
    protected Object readResolve() throws ObjectStreamException {
        try {
            return new PrioritizedSchedulableQuery(this);
        }
        catch (KernelException e) {
            ObjectStreamException e1 = new ObjectStreamException(e.getMessage()) {
            };
            e1.setStackTrace(e.getStackTrace());

            throw e1;
        }
    }

    public void init() {
        attemptToSetBusy();
        querySchedulerJob.init();

        queryRunnerJob.init();
        queryMaster = Registry.getImplFor(QueryMaster.class);

        // ----------

        queryMaster.readyForScheduling(this);
    }

    // -------------

    public QuerySchedulerJob getQuerySchedulerJob() {
        return querySchedulerJob;
    }

    public QueryRunnerJob getQueryRunnerJob() {
        return queryRunnerJob;
    }

    // -------------

    /**
     * @return <code>false</code> if this was already Busy.
     */
    public boolean attemptToSetBusy() {
        return busy.compareAndSet(false, true);
    }

    public boolean isBusy() {
        return busy.get();
    }

    public void setNotBusy() {
        busy.set(false);
    }

    // -------------

    public void beforeCalculateScheduleTime() {
        queryContext.setCurrentTime(timeKeeper.getTimeMsecs());
        scheduleAt = 0;
    }

    public void calculateScheduleTime() {
        QuerySchedulePolicyValue schedulePolicyValue = queryConfig.getQuerySchedulePolicy();

        if (queryConfig.isQueryPaused()) {
            scheduleAt = timeKeeper.getTimeMsecs() + ON_PAUSE_RECHECK_TIME_MILLIS;
        }
        else {
            if (queryConfig.getQueryErrorCount() == 0
                    && schedulePolicyValue.getPolicy() == QuerySchedulePolicy.ATLEAST_OR_SOONER) {
                atleastSoonerPolicyCalc(schedulePolicyValue);
            }
            else {
                fixedPolicyCalc(schedulePolicyValue);
            }
        }
    }

    protected void atleastSoonerPolicyCalc(QuerySchedulePolicyValue schedulePolicyValue) {
        boolean scheduleNow = false;

        final long delay = schedulePolicyValue.getTimeMillis();
        final long lastRanAt = queryConfig.getQueryLastRanAt();
        scheduleAt = (lastRanAt == 0) ? (timeKeeper.getTimeMsecs() + delay) : (lastRanAt + delay);

        // -------------

        // Compare against context-time and not wall-clock time.
        final long contextTime = queryContext.getCurrentTime();
        Set<String> keys = queryContext.getEventExpirationTimeKeys();
        for (String key : keys) {
            SortedSet<Long> timestamps = queryContext.getEventExpirationTimes(key);

            if (timestamps.isEmpty() == false) {
                Long ts = timestamps.first();

                long diff = (contextTime - ts);
                if (diff >= 0) {
                    scheduleNow = true;

                    break;
                }

                scheduleAt = Math.min(ts, scheduleAt);
            }
        }

        // -------------

        if (scheduleNow == false) {
            float pendingRows = 0;

            keys = queryContext.getTotalUnprocessedBufferedRowKeys();
            for (String key : keys) {
                int rows = queryContext.getTotalUnprocessedBufferedRows(key);
                float weight = queryConfig.getUnprocessedEventWeight(key);

                pendingRows = pendingRows + (rows * weight);
                if (pendingRows > 0) {
                    scheduleNow = true;

                    break;
                }
            }
        }

        // -------------

        if (scheduleNow == false) {
            float pendingRows = 0;

            for (FilteredTable filteredTable : filteredTables) {
                int rows = filteredTable.getNumEventsInBucket();
                String key = filteredTable.getSourceTableFQN().getFQN();
                float weight = queryConfig.getUnprocessedEventWeight(key);

                pendingRows = pendingRows + (rows * weight);
                if (pendingRows > 0) {
                    scheduleNow = true;

                    break;
                }
            }
        }

        // -------------

        if (scheduleNow) {
            scheduleAt = timeKeeper.getTimeMsecs();
        }
    }

    protected void fixedPolicyCalc(QuerySchedulePolicyValue schedulePolicyValue) {
        final long time = timeKeeper.getTimeMsecs();
        final long delay = schedulePolicyValue.getTimeMillis();
        final long lastRanAt = queryConfig.getQueryLastRanAt();

        scheduleAt = (lastRanAt == 0) ? (time + delay) : (lastRanAt + delay);
    }

    /**
     * @return Next Query-run time in milliseconds.
     */
    public long afterCalculateScheduleTime() {
        return scheduleAt;
    }

    // -------------

    public void beforeCalculateRunPriority() {
        queryContext.setCurrentTime(timeKeeper.getTimeMsecs());
        queryContext.incrementRunCount();
        priority = 0;
    }

    public void calculateRunPriority() throws Exception {
        // Compare against context-time and not wall-clock time.
        final long contextTime = queryContext.getCurrentTime();
        Set<String> keys = queryContext.getEventExpirationTimeKeys();
        for (String key : keys) {
            SortedSet<Long> timestamps = queryContext.getEventExpirationTimes(key);

            while (timestamps.isEmpty() == false) {
                Long ts = timestamps.first();

                long diff = (contextTime - ts);
                if (diff >= 0) {
                    timestamps.remove(ts);

                    diff = TimeUnit.MILLISECONDS.toSeconds(diff);
                    diff = Math.max(diff, 1);

                    priority = priority + (int) diff;
                }
                else {
                    break;
                }
            }
        }

        // -------------

        float pendingRows = 0;

        // -------------

        keys = queryContext.getTotalUnprocessedBufferedRowKeys();
        for (String key : keys) {
            int rows = queryContext.getTotalUnprocessedBufferedRows(key);
            float weight = queryConfig.getUnprocessedEventWeight(key);

            pendingRows = pendingRows + (rows * weight);
        }

        // -------------

        float accumulatedWeightToSubstract = 0.0f;

        for (FilteredTable filteredTable : filteredTables) {
            int rows = filteredTable.getNumEventsInBucket();

            String key = filteredTable.getSourceTableFQN().getFQN();
            float weight = queryConfig.getUnprocessedEventWeight(key);

            float f = (rows * weight);
            accumulatedWeightToSubstract = accumulatedWeightToSubstract + f;

            pendingRows = pendingRows + f;
        }

        eventWeightLock.lock();
        try {
            accumulatedNewEventWeight = accumulatedNewEventWeight - accumulatedWeightToSubstract;
        }
        finally {
            eventWeightLock.unlock();
        }

        // -------------

        priority = (int) pendingRows;
    }

    /**
     * @return Priority for the Run.
     */
    public int afterCalculateRunPriority() {
        return priority;
    }

    // -------------

    public void querySchedulePolicyChanged(QuerySchedulePolicyValue oldPolicyValue,
            QuerySchedulePolicyValue newPolicyValue) {
        /*
         * If the new policy is supposed to trigger earlier than the old
         * schedule time, then the Query must be re-scheduled.
         */
        if (newPolicyValue.getTimeMillis() < oldPolicyValue.getTimeMillis()) {
            final long time = timeKeeper.getTimeMsecs();
            final long delay = newPolicyValue.getTimeMillis();
            final long lastRanAt = queryConfig.getQueryLastRanAt();

            long forceScheduleTS = (lastRanAt == 0) ? (time + delay) : (lastRanAt + delay);
            attemptForceScheduleQuery(forceScheduleTS);
        }
    }

    /**
     * @return <code>true</code> if lock was acquired. <code>false</code>,
     *         otherwise.
     */
    protected boolean lockNewEventNotifier() {
        return newEventNotifierLock.compareAndSet(false, true);
    }

    protected boolean isNewEventNotifierLocked() {
        return newEventNotifierLock.get();
    }

    protected void unlockNewEventNotifier() {
        newEventNotifierLock.set(false);
    }

    @Override
    public void eventsArrived(EventBucket bucket, int numOfEvents) {
        QuerySchedulePolicyValue schedulePolicyValue = queryConfig.getQuerySchedulePolicy();
        if (schedulePolicyValue.getPolicy() == QuerySchedulePolicy.ATLEAST_OR_SOONER) {
            String key = bucket.getSourceTableFQN().getFQN();
            float weight = queryConfig.getUnprocessedEventWeight(key);

            eventWeightLock.lock();
            try {
                accumulatedNewEventWeight = accumulatedNewEventWeight + (weight * numOfEvents);
            }
            finally {
                eventWeightLock.unlock();
            }

            if (accumulatedNewEventWeight >= 1.0f && isBusy() == false) {
                // Schedule now.
                attemptForceScheduleQuery(timeKeeper.getTimeMsecs());
            }
        }
    }

    protected void attemptForceScheduleQuery(long scheduleTimestamp) {
        if (queryConfig.isQueryPaused() == false && lockNewEventNotifier() == true) {
            try {
                long timeLeft = scheduleAt - timeKeeper.getTimeMsecs();
                /*
                 * Leave this margin to prevent busy-notbusy race conditions.
                 */
                if (timeLeft > queryConfig.getForceScheduleMarginMsecs()
                        && attemptToSetBusy() == true) {
                    try {
                        this.querySchedulerJob.disconnectFromPSQ();
                        this.querySchedulerJob = new QuerySchedulerJob(this);
                        this.querySchedulerJob.init();

                        // ------------

                        scheduleAt = scheduleTimestamp;
                        this.querySchedulerJob.setScheduleTimeMillis(scheduleAt);
                    }
                    finally {
                        queryMaster.readyForScheduling(this);
                    }
                }
            }
            finally {
                unlockNewEventNotifier();
            }
        }
    }

    @Override
    public float getEventWeight(EventBucket bucket) {
        String key = bucket.getSourceTableFQN().getFQN();
        return queryConfig.getUnprocessedEventWeight(key);
    }

    @Override
    public float getTotalCurrentEventWeight() {
        return accumulatedNewEventWeight;
    }
}