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
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
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

/*
 * Author: Ashwin Jayaprakash Date: Apr 8, 2006 Time: 8:50:34 PM
 */

public class JobExecutionManager implements Component {
    protected ThreadPoolExecutor threadPool;

    public void start(Object... params) throws Exception {
        Properties properties = (Properties) params[0];

        String numThreadsStr = properties.getProperty(ConfigKeys.JobExecution.THREADS_NUM);
        int numThreads = Integer.parseInt(numThreadsStr);

        ThreadFactory threadFactory = new DaemonThreadFactory(JobExecutionManager.class
                .getSimpleName()
                + "-Thread-", Thread.MAX_PRIORITY);

        threadPool = new ThreadPoolExecutor((numThreads / 3), numThreads,
                300 /* Reduce size after 5 mins. */, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory);

        // ----------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                JobExecutionManager.class.getName());
        logger.log(Level.INFO, "Started");
    }

    public void stop() throws Exception {
        threadPool.shutdown();
        threadPool = null;

        // ----------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                JobExecutionManager.class.getName());
        logger.log(Level.INFO, "Stopped");
    }

    // ----------------------

    public int getMaximumPoolSize() {
        return threadPool.getMaximumPoolSize();
    }

    public Future<?> submitJob(Runnable runnable) {
        return threadPool.submit(runnable);
    }

    public <T> Future<T> submitJob(Runnable runnable, T result) {
        return threadPool.submit(runnable, result);
    }

    public <T> Future<T> submitJob(Callable<T> callable) {
        return threadPool.submit(callable);
    }
}
