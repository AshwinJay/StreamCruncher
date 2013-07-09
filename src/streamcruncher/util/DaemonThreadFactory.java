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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * Author: Ashwin Jayaprakash Date: Jan 14, 2006 Time: 10:31:40 AM
 */

public class DaemonThreadFactory implements ThreadFactory {
    protected final AtomicInteger threadNumber;

    protected final ThreadGroup group;

    protected final String namePrefix;

    protected final int priority;

    /**
     * Creates Threads with {@link Thread#NORM_PRIORITY} and other properties as
     * listed in {@link #DaemonThreadFactory(String, int)}.
     * 
     * @param namePrefix
     */
    public DaemonThreadFactory(String namePrefix) {
        this(namePrefix, Thread.NORM_PRIORITY);
    }

    /**
     * Creates Threads with specified priority and "daemon" status. Uses the
     * {@link SecurityManager#getThreadGroup()} if the SecurityManager is set,
     * as the base {@link ThreadGroup}.
     * 
     * @param namePrefix
     *            Pool name prefix
     * @param priority
     */
    public DaemonThreadFactory(String namePrefix, int priority) {
        this.threadNumber = new AtomicInteger(1);
        this.namePrefix = namePrefix;
        this.priority = priority;

        SecurityManager s = System.getSecurityManager();
        ThreadGroup threadGroup = (s != null) ? s.getThreadGroup() : Thread.currentThread()
                .getThreadGroup();
        this.group = new ThreadGroup(threadGroup, namePrefix);
    }

    /**
     * @return Returns the group.
     */
    public ThreadGroup getGroup() {
        return group;
    }

    /**
     * @return Returns the namePrefix.
     */
    public String getNamePrefix() {
        return namePrefix;
    }

    /**
     * @return Returns the priority.
     */
    public int getPriority() {
        return priority;
    }

    // -------------------------

    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement(), 0);

        thread.setPriority(priority);
        thread.setDaemon(true);

        return thread;
    }
}
