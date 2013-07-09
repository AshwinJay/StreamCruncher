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

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Registry;
import streamcruncher.innards.core.QueryContext;

/*
 * Author: Ashwin Jayaprakash Date: Apr 9, 2006 Time: 4:45:17 PM
 */

/**
 * Before the Job is scheduled, {@link #setQueryContext(QueryContext)} and
 * optionally, {@link #setCountDownLatch(CountDownLatch)} must be invoked.
 */
public abstract class SimpleJob implements Runnable {
    protected final String name;

    protected final ReusableCountDownLatch countDownLatch;

    protected Throwable throwable;

    protected Thread thread;

    public SimpleJob(String name, ReusableCountDownLatch countDownLatch) {
        this.name = name;
        this.countDownLatch = countDownLatch;
    }

    public String getName() {
        return name;
    }

    /**
     * @return Returns the throwable that occurred in the recent run, if any.
     *         <code>null</code>, if there wasn't any.
     */
    public Throwable getErrorInRun() {
        return throwable;
    }

    /**
     * @return Returns the countDownLatch.
     * @see #setCountDownLatch(CountDownLatch)
     */
    public ReusableCountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    // ---------------

    public void interruptJob() {
        if (thread != null) {
            try {
                thread.interrupt();
            }
            catch (RuntimeException e) {
                Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                        ManagedJob.class.getName());
                logger.log(Level.SEVERE, "Job: " + getName() + " interruption failed", e);
            }
        }
    }

    public final void run() {
        try {
            thread = Thread.currentThread();
            throwable = null;

            doWork();
        }
        catch (Throwable t) {
            throwable = t;
        }
        finally {
            thread = null;
            countDownLatch.countDown();
        }
    }

    protected abstract void doWork() throws Exception;
}
