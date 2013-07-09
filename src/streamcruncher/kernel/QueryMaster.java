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

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Component;
import streamcruncher.boot.ConfigKeys;
import streamcruncher.boot.Registry;
import streamcruncher.util.DaemonThreadFactory;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.RunnableDelayQueue;

/*
 * Author: Ashwin Jayaprakash Date: Aug 13, 2006 Time: 6:47:41 PM
 */

public class QueryMaster implements Component {
    protected RunnableDelayQueue schedulerJobQueue;

    protected PriorityBlockingQueue<Runnable> runnerJobQueue;

    protected ThreadPoolExecutor schedulerJobThreadPool;

    protected ThreadPoolExecutor runnerJobThreadPool;

    protected ConcurrentMap<String, PrioritizedSchedulableQuery> psqMap;

    public void start(Object... params) throws Exception {
        Properties properties = (Properties) params[0];

        // ------------

        schedulerJobQueue = new RunnableDelayQueue();

        String numThreadsStr = properties.getProperty(ConfigKeys.QueryScheduler.THREADS_NUM);
        int numThreads = Integer.parseInt(numThreadsStr);

        ThreadFactory threadFactory = new DaemonThreadFactory(QuerySchedulerJob.class
                .getSimpleName()
                + "-Thread-", Thread.MAX_PRIORITY);

        /*
         * No timeout for Pool and pre-start one Thread, otherwise the Pool runs
         * the submitted job by spawning a new Thread and bypassing the
         * BlockingQueue (no Delay), directly.
         */
        schedulerJobThreadPool = new ThreadPoolExecutor(1, numThreads, 0, TimeUnit.SECONDS,
                schedulerJobQueue, threadFactory);
        schedulerJobThreadPool.prestartCoreThread();

        // ------------

        runnerJobQueue = new PriorityBlockingQueue<Runnable>();

        numThreadsStr = properties.getProperty(ConfigKeys.QueryRunner.THREADS_NUM);
        numThreads = Integer.parseInt(numThreadsStr);

        threadFactory = new DaemonThreadFactory(QueryRunnerJob.class.getSimpleName() + "-Thread-",
                Thread.MAX_PRIORITY);

        runnerJobThreadPool = new ThreadPoolExecutor(1, numThreads,
        /* 5 minutes. */300, TimeUnit.SECONDS, runnerJobQueue, threadFactory);
        runnerJobThreadPool.prestartCoreThread();

        // ------------

        // 2 Thread concurrency.
        psqMap = new ConcurrentHashMap<String, PrioritizedSchedulableQuery>(2);

        // ------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                QueryMaster.class.getName());
        logger.log(Level.INFO, "Started");
    }

    public void stop() throws Exception {
        schedulerJobQueue.clear();
        runnerJobQueue.clear();

        psqMap.clear();

        schedulerJobThreadPool.shutdown();
        runnerJobThreadPool.shutdown();

        // ------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                QueryMaster.class.getName());
        logger.log(Level.INFO, "Stopped");
    }

    // ------------

    public void registerQuery(PrioritizedSchedulableQuery psq) {
        psqMap.put(psq.getName(), psq);

        psq.init();
    }

    public PrioritizedSchedulableQuery getScheduledQuery(String name) {
        return psqMap.get(name);
    }

    public void unregisterQuery(String name) {
        PrioritizedSchedulableQuery psq = psqMap.get(name);
        if (psq != null) {
            // Prevent notifications etc.
            psq.attemptToSetBusy();

            psq.discard();
        }

        psqMap.remove(name);
    }

    // ------------

    /**
     * @param psq
     *            {@link PrioritizedSchedulableQuery#isBusy()} must be
     *            <code>true</code> when this method is invoked.
     */
    public void readyForScheduling(PrioritizedSchedulableQuery psq) {
        if (psqMap.get(psq.getName()) == psq) {
            if (psq.isBusy()) {
                // Store the reference before setting as Not-Busy.
                QuerySchedulerJob schedulerJob = psq.getQuerySchedulerJob();
                psq.setNotBusy();

                schedulerJobThreadPool.execute(schedulerJob);
            }
        }
    }

    public void readyForRunning(PrioritizedSchedulableQuery psq) {
        if (psqMap.get(psq.getName()) == psq) {
            QueryRunnerJob runnerJob = psq.getQueryRunnerJob();
            runnerJobThreadPool.execute(runnerJob);
        }
    }
}
