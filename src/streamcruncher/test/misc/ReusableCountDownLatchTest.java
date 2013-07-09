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

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import streamcruncher.test.TestGroupNames;
import streamcruncher.util.ReusableCountDownLatch;

/*
 * Author: Ashwin Jayaprakash Date: Nov 4, 2006 Time: 1:26:33 PM
 */

public class ReusableCountDownLatchTest {
    @Test(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, groups = { TestGroupNames.SC_TEST })
    public void test() {
        ReusableCountDownLatch latch = new ReusableCountDownLatch();

        for (int i = 0; i < 100; i++) {
            try {
                runCycle(latch);
            }
            catch (InterruptedException e) {
                Assert.fail("Count down did not work", e);
            }
        }
    }

    public void runCycle(final ReusableCountDownLatch latch) throws InterruptedException {

        final int count = 2;
        final boolean[] allCompleted = new boolean[count];

        latch.startNewCycle(count);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(700);
                }
                catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                latch.countDown();

                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                allCompleted[0] = true;
            }
        });

        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(700);
                }
                catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                latch.countDown();

                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                allCompleted[1] = true;
            }
        });

        Assert.assertEquals(latch.getRemaining(), count, "Start count is not correct");

        t1.start();
        t2.start();

        long start = System.nanoTime();
        final int remaining = latch.await(6, TimeUnit.SECONDS);
        System.err.println("Completed in: " + (System.nanoTime() - start) / 1000000);
        Assert.assertEquals(remaining, 0, "Count down did not work");

        t1.join();
        t2.join();
        Assert.assertTrue(allCompleted[0], "Await[0] did not work");
        Assert.assertTrue(allCompleted[1], "Await[1] did not work");
    }
}
