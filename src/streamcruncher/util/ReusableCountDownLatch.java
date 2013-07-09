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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Registry;
import streamcruncher.util.sysevent.SystemEvent;
import streamcruncher.util.sysevent.SystemEventBus;
import streamcruncher.util.sysevent.SystemEvent.Priority;

/*
 * Author: Ashwin Jayaprakash Date: Nov 4, 2006 Time: 12:56:39 PM
 */

/**
 * doc For some strange reason, the Latch sometimes does not decrement
 * correctly. This has happened several times in "build 1.6.0-beta2-b82".
 * Sometimes, even after all the Threads have decremented correctly, the
 * await(xxx) never returns. Othertimes it just pauses for several seconds even
 * after all jobs have finished.
 */
public class ReusableCountDownLatch {
    protected static final int defaultAwaitLoopTimeSecs = 10;

    protected final AtomicInteger counter;

    protected final Lock lock;

    protected final Condition condition;

    public ReusableCountDownLatch() {
        this.counter = new AtomicInteger();
        this.lock = new ReentrantLock();
        this.condition = this.lock.newCondition();
    }

    /**
     * Resets the counter.
     * 
     * @param counter
     */
    public void startNewCycle(int counter) {
        this.counter.set(counter);
    }

    // --------

    /**
     * @return The current counter value.
     */
    public int getRemaining() {
        return counter.get();
    }

    /**
     * Decrements the counter by 1.
     */
    public void countDown() {
        int newVal = counter.decrementAndGet();

        if (newVal <= 0) {
            try {
                lock.lock();

                condition.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        if (newVal < 0) {
            Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                    ReusableCountDownLatch.class.getName());

            String msg = "Count down latch was already 0. Now it is: " + newVal
                    + ". Details - Thread" + Thread.currentThread().getName() + ", StackTrace: ";
            Exception stackTrace = new Exception();

            logger.log(Level.WARNING, msg, stackTrace);

            SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
            SystemEvent event = new SystemEvent(ReusableCountDownLatch.class.getName(), msg,
                    stackTrace, Priority.WARNING);
            bus.submit(event);
        }
    }

    public void await() throws InterruptedException {
        while (await(defaultAwaitLoopTimeSecs, TimeUnit.SECONDS) > 0) {
        }
    }

    /**
     * @param duration
     * @param timeUnit
     * @return The counter value that is remaining.
     * @throws InterruptedException
     */
    public int await(long duration, TimeUnit timeUnit) throws InterruptedException {
        long nanos = timeUnit.toNanos(duration);

        while (nanos > 0) {
            if (getRemaining() <= 0) {
                break;
            }

            try {
                lock.lock();

                nanos = condition.awaitNanos(nanos);

                condition.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        return getRemaining();
    }
}
