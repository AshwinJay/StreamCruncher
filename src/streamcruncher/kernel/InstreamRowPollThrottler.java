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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import streamcruncher.boot.Registry;
import streamcruncher.innards.InnardsManager;
import streamcruncher.innards.core.InstreamNotificationRendezvous;
import streamcruncher.innards.core.stream.InStream;

/*
 * Author: Ashwin Jayaprakash Date: Apr 22, 2006 Time: 3:54:21 PM
 */

public class InstreamRowPollThrottler {
    protected final int emptyRunsBeforePause;

    protected final long pauseMsecs;

    protected final Random random;

    protected final InstreamNotificationRendezvous notificationRendezvous;

    protected int runsWithoutSuccess;

    public InstreamRowPollThrottler(int emptyRunsBeforePause, long pauseMsecs) {
        this.emptyRunsBeforePause = emptyRunsBeforePause;
        this.pauseMsecs = pauseMsecs;
        this.random = new Random();

        InnardsManager manager = Registry.getImplFor(InnardsManager.class);
        this.notificationRendezvous = manager.getNotificationRendezvous();
    }

    // -----------------

    /**
     * Analyse Stream's rate of flow or Trend. Then, reward or penalize.
     * 
     * @param rowsPickedUp
     */
    public void afterProcess(int rowsPickedUp) {
        if (rowsPickedUp > 0) {
            // Speed up slowly.
            runsWithoutSuccess = (int) (runsWithoutSuccess * 0.5);
        }
        else {
            // Penalize.
            runsWithoutSuccess++;

            /*
             * For [0 to 9] > 6, is [7, 8, 9] = 30% prob. Reduce penalty only
             * 30% of the time.
             */
            if (random.nextInt(10) > 6) {
                runsWithoutSuccess = (int) (runsWithoutSuccess * 0.7);
            }
        }
    }

    /**
     * @return {@link InStream} to process. <code>null</code> if nothing in
     *         particular to process.
     */
    public InStream throttle() {
        InStream inStreamToProcess = null;

        if (runsWithoutSuccess >= emptyRunsBeforePause) {
            /*
             * "int" is used purposely to get exact multiples of
             * emptyRunsBeforePause.
             */
            int stepFactor = runsWithoutSuccess / emptyRunsBeforePause;

            if (stepFactor > 0) {
                inStreamToProcess = notificationRendezvous.awaitNotification(pauseMsecs
                        * stepFactor, TimeUnit.MILLISECONDS);
            }
        }

        return inStreamToProcess;
    }
}
