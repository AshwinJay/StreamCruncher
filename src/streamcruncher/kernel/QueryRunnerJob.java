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

import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.api.artifact.TableFQN;
import streamcruncher.boot.Registry;
import streamcruncher.innards.InnardsManager;
import streamcruncher.innards.core.stream.OutStream;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.sysevent.SystemEvent;
import streamcruncher.util.sysevent.SystemEventBus;
import streamcruncher.util.sysevent.SystemEvent.Priority;

/*
 * Author: Ashwin Jayaprakash Date: Aug 13, 2006 Time: 8:13:36 PM
 */

public class QueryRunnerJob implements Runnable, Comparable<QueryRunnerJob> {
    protected final PrioritizedSchedulableQuery psq;

    protected final QueryConfig queryConfig;

    protected QueryMaster queryMaster;

    protected InnardsManager dbManager;

    protected int priority;

    protected OutStream outStream;

    public QueryRunnerJob(PrioritizedSchedulableQuery psq) {
        this.psq = psq;
        this.queryConfig = psq.getQueryConfig();
    }

    public void init() {
        queryMaster = Registry.getImplFor(QueryMaster.class);
        dbManager = Registry.getImplFor(InnardsManager.class);
    }

    // --------------

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    // --------------

    public void run() {
        try {
            doRun();

            psq.beforeCalculateScheduleTime();
            psq.calculateScheduleTime();
            long scheduleTime = psq.afterCalculateScheduleTime();

            QuerySchedulerJob schedulerJob = psq.getQuerySchedulerJob();
            schedulerJob.setScheduleTimeMillis(scheduleTime);
        }
        finally {
            queryMaster.readyForScheduling(psq);
        }
    }

    protected void doRun() {
        QueryOutput output = null;
        try {
            psq.prepareToRun();

            try {
                output = psq.run();
            }
            finally {
                try {
                    psq.afterRun();
                }
                catch (Throwable t) {
                    Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                            QueryRunnerJob.class.getName());
                    logger.log(Level.WARNING,
                            "An error occurred while cleaning-up after (possibly) the Query ran: "
                                    + psq.getName(), t);
                }
            }
        }
        catch (Throwable t) {
            // todo What about rollbacks?

            Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                    QueryRunnerJob.class.getName());
            logger.log(Level.SEVERE, "An error occurred while executing the Query: "
                    + psq.getName(), t);

            SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
            SystemEvent event = new SystemEvent(QueryRunnerJob.class.getName(), psq.getName(), t,
                    Priority.SEVERE);
            bus.submit(event);

            // -------------

            queryConfig.incrementQueryErrorCount();
            return;
        }

        // -----------------------

        if (output != null) {
            TableFQN tableFQN = psq.getResultTableFQN();
            if (outStream == null) {
                outStream = dbManager.getRegisteredOutStream(psq.getName());
            }

            outStream.addNextWindowRange(output);
        }

        // -----------------------

        queryConfig.queryRunSucceeded();
    }

    public int compareTo(QueryRunnerJob that) {
        int c = getPriority() - that.getPriority();

        return (c == 0) ? 0 : ((c < 0) ? -1 : 1);
    }
}
