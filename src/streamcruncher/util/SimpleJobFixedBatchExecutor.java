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

import java.util.IdentityHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Registry;
import streamcruncher.kernel.JobExecutionManager;
import streamcruncher.kernel.QueryRunnerJob;
import streamcruncher.util.sysevent.SystemEvent;
import streamcruncher.util.sysevent.SystemEventBus;
import streamcruncher.util.sysevent.SystemEvent.Priority;

/*
 * Author: Ashwin Jayaprakash Date: Apr 9, 2006 Time: 5:04:03 PM
 */

public class SimpleJobFixedBatchExecutor {
    public static final int STUCK_JOB_CHECK_DIVISOR = 2;

    protected final JobExecutionManager executionManager;

    protected final SimpleJob[] jobs;

    protected final String name;

    protected final IdentityHashMap<Future, SimpleJob> futures;

    protected final ReusableCountDownLatch countDownLatch;

    protected final Logger logger;

    /**
     * Must be set before invoking {@link #runJobs()}, if it has to reflect in
     * that cycle.
     */
    protected long stuckJobInterruptionTimeMsecs = 20 * 1000;

    public SimpleJobFixedBatchExecutor(SimpleJob[] jobs, String name,
            ReusableCountDownLatch countDownLatch) {
        this.executionManager = Registry.getImplFor(JobExecutionManager.class);
        this.jobs = jobs;
        this.name = name;
        this.futures = new IdentityHashMap<Future, SimpleJob>();
        this.countDownLatch = countDownLatch;

        this.logger = Registry.getImplFor(LoggerManager.class).getLogger(
                SimpleJobFixedBatchExecutor.class.getName());
    }

    /**
     * @return Returns the jobs.
     */
    public SimpleJob[] getJobs() {
        return jobs;
    }

    public String getName() {
        return name;
    }

    public long getStuckJobInterruptionTimeMsecs() {
        return stuckJobInterruptionTimeMsecs;
    }

    public void setStuckJobInterruptionTimeMsecs(long stuckJobInterruptionTimeMsecs) {
        this.stuckJobInterruptionTimeMsecs = stuckJobInterruptionTimeMsecs;
    }

    // -------------------

    /**
     * This method will return only <b>after</b> all the Jobs have completed
     * (successfully or with Exceptions).
     * 
     * @throws SimpleJobBatchExecutionException
     */
    public void runJobs() throws SimpleJobBatchExecutionException {
        countDownLatch.startNewCycle(jobs.length);
        futures.clear();

        try {
            for (SimpleJob job : jobs) {
                Future future = executionManager.submitJob(job);
                futures.put(future, job);
            }
        }
        finally {
            try {
                handleLatchAwait();
            }
            catch (InterruptedException e) {
                throw new SimpleJobBatchExecutionException(e);
            }
        }

        // ---------------

        SimpleJobBatchExecutionException batchExecutionException = null;

        for (SimpleJob job : jobs) {
            Throwable throwable = job.getErrorInRun();

            if (throwable != null) {
                if (batchExecutionException == null) {
                    batchExecutionException = new SimpleJobBatchExecutionException(
                            "Error(s) occurred while executing the Jobs.");
                }

                batchExecutionException.addError(job.getName(), throwable);

                // -----------

                SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
                SystemEvent event = new SystemEvent(QueryRunnerJob.class.getName(), getName() + "."
                        + job.getName(), throwable, Priority.SEVERE);
                bus.submit(event);
            }
        }

        if (batchExecutionException != null) {
            throw batchExecutionException;
        }
    }

    protected void handleLatchAwait() throws InterruptedException {
        final long recheckTimeMsecs = stuckJobInterruptionTimeMsecs / STUCK_JOB_CHECK_DIVISOR;
        int count = 0;

        while (countDownLatch.await(recheckTimeMsecs, TimeUnit.MILLISECONDS) > 0) {
            count++;
            long time = (count * recheckTimeMsecs);
            String msg = "Jobs in Batch: " + getName() + " are still running even after: " + time
                    + " milliseconds.";

            int notDone = 0;
            for (Future future : futures.keySet()) {
                SimpleJob job = futures.get(future);

                msg = msg + " [" + job.getName() + " Completed: " + future.isDone() + "]";

                if (future.isDone() == false) {
                    notDone++;
                }
            }

            Priority eventPriority = Priority.WARNING;
            if (count >= STUCK_JOB_CHECK_DIVISOR) {
                logger.log(Level.SEVERE, msg);

                msg = "Interrupting Jobs that are still running in Batch: " + getName();
                for (Future future : futures.keySet()) {
                    if (future.isDone() == false) {
                        boolean cancelled = future.cancel(true);

                        SimpleJob job = futures.get(future);
                        msg = msg + " [" + job.getName() + " Cancelled: " + cancelled + "]";
                    }
                }

                logger.log(Level.SEVERE, "Interrupting Jobs that are still running in Batch: "
                        + getName());
                eventPriority = Priority.SEVERE;
            }
            else {
                logger.log(Level.WARNING, msg);
            }

            SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
            SystemEvent event = new SystemEvent(QueryRunnerJob.class.getName(), getName(), msg,
                    eventPriority);
            bus.submit(event);
        }
    }
}
