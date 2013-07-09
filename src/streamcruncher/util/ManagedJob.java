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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Registry;

/*
 * Author: Ashwin Jayaprakash Date: Jan 16, 2006 Time: 10:22:48 PM
 */

public abstract class ManagedJob implements UncaughtExceptionHandler {
    public static final long STOP_JOB_WAIT_MSECS_BEFORE_INTERRUPT = 10000;

    protected final ThreadFactory threadFactory;

    protected final boolean restartOnException;

    protected volatile Thread currentThread;

    /**
     * Restarts on Exception.
     * 
     * @param threadFactory
     * @see #ManagedJob(ThreadFactory, boolean)
     */
    public ManagedJob(ThreadFactory threadFactory) {
        this(threadFactory, true);
    }

    /**
     * @param threadFactory
     * @param restartOnException
     */
    public ManagedJob(ThreadFactory threadFactory, boolean restartOnException) {
        this.threadFactory = threadFactory;
        this.restartOnException = restartOnException;
    }

    // -----------------------

    public void startJob() {
        Runnable runnable = new Runnable() {
            public void run() {
                ManagedJob.this.performJob();
            }
        };

        currentThread = threadFactory.newThread(runnable);
        currentThread.setUncaughtExceptionHandler(this);
        currentThread.start();
    }

    protected abstract void performJob();

    /**
     * <b>Note:</b> Must return immediately.
     */
    protected abstract void requestStop();

    /**
     * Invokes {@link #requestStop()} and waits for
     * {@link #STOP_JOB_WAIT_MSECS_BEFORE_INTERRUPT} before interrupting the
     * Thread.
     * 
     * @throws InterruptedException
     */
    public void stopJob() throws InterruptedException {
        requestStop();

        while (currentThread != null && currentThread.isAlive()) {
            currentThread.join(STOP_JOB_WAIT_MSECS_BEFORE_INTERRUPT);

            requestStop();
            try {
                currentThread.interrupt();
            }
            catch (RuntimeException e) {
                Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                        ManagedJob.class.getName());
                logger.log(Level.SEVERE, "Job interruption failed", e);
            }
        }
    }

    // -----------------------

    public void uncaughtException(Thread thread, Throwable throwable) {
        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                ManagedJob.class.getName());
        logger.log(Level.SEVERE, "Thread death: " + thread.getName(), throwable);

        // -----------------------

        currentThread = null;

        if (restartOnException) {
            startJob();
        }
    }
}
