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
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Component;
import streamcruncher.boot.ConfigKeys;
import streamcruncher.boot.Registry;
import streamcruncher.kernel.InStreamAddRowEventProcessor.SharedData;
import streamcruncher.util.DaemonThreadFactory;
import streamcruncher.util.LoggerManager;

/*
 * Author: Ashwin Jayaprakash Date: Jan 4, 2006 Time: 8:42:54 PM
 */

public class InStreamAddRowEventManager implements Component {
    protected InStreamAddRowEventProcessor[] processors;

    protected SharedData sharedData;

    // ------------------------

    /**
     * @param params
     */
    public void start(Object... params) throws Exception {
        Properties properties = (Properties) params[0];

        String numThreads = properties
                .getProperty(ConfigKeys.InStreamEventProcessor.PROCESSOR_THREADS_NUM);
        int numOfProcessorThreads = Integer.parseInt(numThreads);

        ThreadFactory threadFactory = new DaemonThreadFactory(InStreamAddRowEventProcessor.class
                .getSimpleName()
                + "-Thread-", Thread.MIN_PRIORITY);

        String runsBeforePauseStr = properties
                .getProperty(ConfigKeys.InStreamEventProcessor.PROCESSOR_THREAD_EMPTY_RUNS_BEFORE_PAUSE);
        int runsBeforePause = Integer.parseInt(runsBeforePauseStr);

        String pauseMsecsStr = properties
                .getProperty(ConfigKeys.InStreamEventProcessor.PROCESSOR_THREAD_PAUSE_MSECS);
        long pauseMsecs = Long.parseLong(pauseMsecsStr);

        // ----------------------

        processors = new InStreamAddRowEventProcessor[numOfProcessorThreads];

        sharedData = new SharedData();
        for (int i = 0; i < numOfProcessorThreads; i++) {
            InStreamAddRowEventProcessor eventProcessor = new InStreamAddRowEventProcessor(
                    threadFactory, sharedData, runsBeforePause, pauseMsecs);

            processors[i] = eventProcessor;
            processors[i].startJob();
        }

        // ----------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                InStreamAddRowEventManager.class.getName());
        logger.log(Level.INFO, "Started");
    }

    /**
     * Waits for all the Processor-Threads to stop.
     * 
     * @throws Exception
     */
    public void stop() throws Exception {
        for (InStreamAddRowEventProcessor processor : processors) {
            processor.stopJob();
        }
        processors = null;
        sharedData = null;

        // ----------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                InStreamAddRowEventManager.class.getName());
        logger.log(Level.INFO, "Stopped");
    }

    // ----------------------

    public void refreshCachedInStreams() {
        if (sharedData != null) {
            sharedData.refreshCachedInstreams();
        }
    }
}
