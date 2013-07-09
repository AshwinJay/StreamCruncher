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
package streamcruncher.test.misc;

import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import streamcruncher.kernel.QuerySchedulerJob;
import streamcruncher.test.TestGroupNames;
import streamcruncher.util.DaemonThreadFactory;
import streamcruncher.util.DelayedRunnable;
import streamcruncher.util.RunnableDelayQueue;

/*
 * Author: Ashwin Jayaprakash Date: Sep 12, 2006 Time: 6:08:56 PM
 */

public class RunnableDelayQueueTest {
    @Test(groups = { TestGroupNames.SC_TEST })
    public void test() {
        RunnableDelayQueue schedulerJobQueue = new RunnableDelayQueue();

        ThreadFactory threadFactory = new DaemonThreadFactory(QuerySchedulerJob.class
                .getSimpleName()
                + "-Thread-", Thread.MAX_PRIORITY);

        ThreadPoolExecutor schedulerJobThreadPool = new ThreadPoolExecutor(1, Integer.MAX_VALUE,
        /* No timeout */0, TimeUnit.SECONDS, schedulerJobQueue, threadFactory);
        // Otherwise, first job runs by skipping getDelay.
        schedulerJobThreadPool.prestartCoreThread();

        DelayedRunnableImpl impl = new DelayedRunnableImpl();
        schedulerJobThreadPool.execute(impl);

        try {
            Thread.sleep(impl.getDelay(TimeUnit.MILLISECONDS) + 3000);
        }
        catch (InterruptedException e) {
            e.printStackTrace(System.err);
        }
        schedulerJobThreadPool.shutdownNow();

        Assert.assertEquals(schedulerJobThreadPool.getCompletedTaskCount(), 1, "Task didn't run!!");
        long diff = Math.abs(impl.getRanAt() - impl.getScheduleTimeMillis());
        Assert.assertTrue(diff < 2000, "Task ran sooner or later than 2 sec diff");
    }

    public static class DelayedRunnableImpl implements DelayedRunnable {
        long scheduleTimeMillis;

        long ranAt;

        public DelayedRunnableImpl() {
            scheduleTimeMillis = System.currentTimeMillis() + 10000;
        }

        public long getRanAt() {
            return ranAt;
        }

        public long getScheduleTimeMillis() {
            return scheduleTimeMillis;
        }

        public long getDelay(TimeUnit unit) {
            long delay = scheduleTimeMillis - System.currentTimeMillis();

            System.err.println("Delay:" + unit.convert(delay, TimeUnit.MILLISECONDS));

            return delay;
        }

        public int compareTo(Delayed that) {
            long d = getDelay(TimeUnit.MILLISECONDS) - that.getDelay(TimeUnit.MILLISECONDS);
            return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
        }

        public void run() {
            System.err.println("Running!");

            ranAt = System.currentTimeMillis();
        }
    }
}
