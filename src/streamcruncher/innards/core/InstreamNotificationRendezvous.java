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
package streamcruncher.innards.core;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Registry;
import streamcruncher.innards.core.stream.InStream;
import streamcruncher.util.LoggerManager;

/*
 * Author: Ashwin Jayaprakash Date: Jun 4, 2006 Time: 4:11:22 PM
 */

public class InstreamNotificationRendezvous implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final Lock lock;

    protected final Condition notificationCondition;

    protected final AtomicInteger waitingThreads;

    /**
     * Must be accessed only inside {@link #lock}.
     */
    protected transient InStream handOff;

    public InstreamNotificationRendezvous() {
        this.lock = new ReentrantLock();
        this.notificationCondition = lock.newCondition();
        this.waitingThreads = new AtomicInteger(0);
    }

    /**
     * @param time
     * @param timeUnit
     * @return <code>null</code> if there was no specific {@link InStream} to
     *         process.
     */
    public InStream awaitNotification(long time, TimeUnit timeUnit) {
        InStream inStream = null;
        waitingThreads.incrementAndGet();

        lock.lock();
        try {
            // Wait only if there is nothing to process.
            if (handOff == null) {
                notificationCondition.await(time, timeUnit);
            }

            /*
             * Spurious wakeups are ok. Therefore, no while(handOff == null)
             * loop.
             */
            inStream = handOff;
            handOff = null;
        }
        catch (InterruptedException e) {
            Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                    InstreamNotificationRendezvous.class.getName());
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        finally {
            lock.unlock();
        }

        waitingThreads.decrementAndGet();
        return inStream;
    }

    public void sendNotification(InStream inStream) {
        if (waitingThreads.get() > 0) {
            lock.lock();
            try {
                handOff = inStream;
                notificationCondition.signal();
            }
            finally {
                lock.unlock();
            }
        }
    }
}
