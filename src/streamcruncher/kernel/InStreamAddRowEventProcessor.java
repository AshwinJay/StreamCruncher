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

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import streamcruncher.boot.Registry;
import streamcruncher.innards.InnardsManager;
import streamcruncher.innards.core.EventBucket;
import streamcruncher.innards.core.stream.InStream;
import streamcruncher.innards.core.stream.InStreamListenerData;
import streamcruncher.util.ManagedJob;
import streamcruncher.util.SmartArrayList;

/*
 * Author: Ashwin Jayaprakash Date: Jan 4, 2006 Time: 8:03:37 PM
 */

public class InStreamAddRowEventProcessor extends ManagedJob {
    protected static class SharedData {
        protected final ReentrantReadWriteLock cacheLock;

        protected final SmartArrayList<InStream> cachedJobs;

        protected final IdentityHashMap<InStream, Long> cachedJobsAndUnprocessedEvents;

        protected final IdentityHashMap<InStream, Integer> cachedJobPositions;

        protected final IdentityHashMap<InStream, InStreamListenerData[]> cachedListeners;

        protected final HashMap<String, PrioritizedSchedulableQuery> cachedPSQs;

        protected AtomicBoolean refreshRequired;

        public SharedData() {
            cacheLock = new ReentrantReadWriteLock();

            cachedJobs = new SmartArrayList<InStream>();
            cachedJobsAndUnprocessedEvents = new IdentityHashMap<InStream, Long>();
            cachedJobPositions = new IdentityHashMap<InStream, Integer>();

            cachedListeners = new IdentityHashMap<InStream, InStreamListenerData[]>();
            cachedPSQs = new HashMap<String, PrioritizedSchedulableQuery>();

            refreshRequired = new AtomicBoolean(false);
        }

        public void refreshCachedInstreams() {
            refreshRequired.set(true);
        }
    }

    // -----------------------

    protected final SharedData sharedData;

    protected final AtomicBoolean stopFlag;

    protected final int emptyRunsBeforePause;

    protected final long pauseMsecs;

    protected final InstreamRowPollThrottler throttleControl;

    protected final Random random;

    // -----------------------

    protected QueryMaster queryMaster;

    protected InStream currStream;

    /**
     * @param threadFactory
     * @param sharedData
     * @param emptyRunsBeforePause
     * @param pauseMsecs
     */
    protected InStreamAddRowEventProcessor(ThreadFactory threadFactory, SharedData sharedData,
            int emptyRunsBeforePause, long pauseMsecs) {
        super(threadFactory);

        this.sharedData = sharedData;
        this.emptyRunsBeforePause = emptyRunsBeforePause;
        this.pauseMsecs = pauseMsecs;
        this.stopFlag = new AtomicBoolean(false);
        this.random = new Random();

        this.throttleControl = new InstreamRowPollThrottler(emptyRunsBeforePause, pauseMsecs);
    }

    // ----------------------

    @Override
    protected void performJob() {
        keepScanningJobs();
    }

    @Override
    protected void requestStop() {
        stopFlag.set(true);
    }

    // ----------------------

    protected void refreshInStreamCache() {
        try {
            try {
                sharedData.cacheLock.readLock().unlock();
            }
            finally {
                sharedData.cacheLock.writeLock().lock();
            }

            // Already refreshed by first Thread that acquired Write-lock.
            if (sharedData.refreshRequired.compareAndSet(true, false) == false) {
                return;
            }

            InnardsManager manager = Registry.getImplFor(InnardsManager.class);
            InStream[] streams = {};
            // Take a snapshot.
            streams = manager.getAllRegisteredInStreams().values().toArray(streams);

            // ------------------------

            sharedData.cachedJobs.clear();
            sharedData.cachedJobs.trimToSize();
            sharedData.cachedJobsAndUnprocessedEvents.clear();
            sharedData.cachedJobPositions.clear();

            sharedData.cachedListeners.clear();

            sharedData.cachedPSQs.clear();

            // ------------------------

            if (queryMaster == null) {
                queryMaster = Registry.getImplFor(QueryMaster.class);
            }

            for (InStream stream : streams) {
                stream.unsetNotificationLock();
                sharedData.cachedJobs.add(stream);

                Map<String, InStreamListenerData> map = stream.getListeners();
                InStreamListenerData[] data = {};
                data = map.values().toArray(data);

                sharedData.cachedListeners.put(stream, data);

                for (InStreamListenerData listener : data) {
                    PrioritizedSchedulableQuery psq = queryMaster.getScheduledQuery(listener
                            .getRunningSelectQueryName());
                    sharedData.cachedPSQs.put(psq.getName(), psq);
                }
            }

            // ------------------------

            // Give every Job, an equal opportunity.
            Collections.shuffle(sharedData.cachedJobs);
            int k = 0;
            for (InStream aStream : sharedData.cachedJobs) {
                sharedData.cachedJobPositions.put(aStream, k);
                k++;
            }
        }
        finally {
            try {
                sharedData.cacheLock.readLock().lock();
            }
            finally {
                sharedData.cacheLock.writeLock().unlock();
            }
        }
    }

    // ----------------------

    protected void keepScanningJobs() {
        stopFlag.set(false);

        InStream urgentNotifiedStream = null;

        while (stopFlag.get() != true) {
            sharedData.cacheLock.readLock().lock();
            try {
                if (sharedData.refreshRequired.get() == true) {
                    refreshInStreamCache();
                }

                // -------------

                currStream = null;

                // -------------

                int size = sharedData.cachedJobs.size();

                Integer position = null;
                if (urgentNotifiedStream != null) {
                    position = sharedData.cachedJobPositions.get(urgentNotifiedStream);
                }

                int itemPosition = 0;
                if (size > 0) {
                    if (position == null || position.intValue() >= size) {
                        itemPosition = random.nextInt(size);
                    }
                    else {
                        itemPosition = position;
                    }
                }

                try {
                    // Run one full round, starting from that position.
                    for (int itemsChecked = 0; itemsChecked < size; itemsChecked++) {
                        currStream = sharedData.cachedJobs.get(itemPosition);

                        /*
                         * Each InStream can be processed by one Thread only.
                         * Otherwise, the events that arrived in-order at the
                         * Stream will be replicated out-of-order if processed
                         * by multiple Threads.
                         */
                        if (currStream.attemptNotificationLock()) {
                            break;
                        }

                        currStream = null;

                        itemPosition = (itemPosition + 1) % size;
                    }
                }
                finally {
                    int rowsProcessed = 0;
                    try {
                        if (currStream != null) {
                            try {
                                rowsProcessed = process();
                            }
                            finally {
                                currStream.unsetNotificationLock();
                            }
                        }
                    }
                    finally {
                        throttleControl.afterProcess(rowsProcessed);
                    }
                }
            }
            finally {
                sharedData.cacheLock.readLock().unlock();
            }

            urgentNotifiedStream = throttleControl.throttle();
            if (urgentNotifiedStream == null) {
                Thread.yield();
            }
        }
    }

    /**
     * @return Rows picked up.
     */
    protected int process() {
        Long prevSize = sharedData.cachedJobsAndUnprocessedEvents.get(currStream);
        long currSize = currStream.getStreamData().getSize();
        int total = (int) (currSize - ((prevSize == null) ? 0 : prevSize));
        sharedData.cachedJobsAndUnprocessedEvents.put(currStream, currSize);

        InStreamListenerData[] listeners = sharedData.cachedListeners.get(currStream);
        for (InStreamListenerData streamListenerData : listeners) {
            EventBucket[] buckets = streamListenerData.getBuckets();
            for (EventBucket bucket : buckets) {
                bucket.eventsReceived();
            }
        }

        return total;
    }
}
