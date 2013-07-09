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

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Component;
import streamcruncher.boot.ConfigKeys;
import streamcruncher.boot.Registry;
import streamcruncher.innards.InnardsManager;
import streamcruncher.innards.core.stream.InStream;
import streamcruncher.util.DaemonThreadFactory;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.ManagedJob;

/*
 * Author: Ashwin Jayaprakash Date: Jan 19, 2006 Time: 11:17:59 AM
 */

public class RowDisposer implements Component {
    protected AtomicBoolean stopFlag;

    protected long disposerPauseTimeMsecs;

    protected ManagedJob internalJob;

    // ---------------

    protected void keepDisposing() {
        stopFlag.set(false);

        while (stopFlag.get() == false) {
            InnardsManager manager = Registry.getImplFor(InnardsManager.class);

            ConcurrentMap<String, InStream> map = manager.getAllRegisteredInStreams();
            Collection<InStream> collection = map.values();
            InStream[] streams = {};
            // Take a snapshot.
            streams = collection.toArray(streams);

            for (InStream stream : streams) {
                stream.getStreamData().removeUnusedOldFragments();
            }

            // ---------------

            try {
                Thread.sleep(disposerPauseTimeMsecs);
            }
            catch (InterruptedException e) {
                if (stopFlag.get() == false) {
                    Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                            RowDisposer.class.getName());
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
            }
        }
    }

    // ---------------

    public void start(Object... params) throws Exception {
        Properties properties = (Properties) params[0];

        ThreadFactory threadFactory = new DaemonThreadFactory(RowDisposer.class.getSimpleName()
                + "-Thread-", Thread.MIN_PRIORITY);

        // ---------------

        stopFlag = new AtomicBoolean(false);
        String pauseTimeMsecsStr = properties
                .getProperty(ConfigKeys.RowDisposer.DISPOSER_THREAD_PAUSE_MSECS);
        disposerPauseTimeMsecs = Long.parseLong(pauseTimeMsecsStr);

        internalJob = new ManagedJob(threadFactory) {
            @Override
            protected void performJob() {
                keepDisposing();
            }

            @Override
            protected void requestStop() {
                stopFlag.set(true);
            }
        };

        internalJob.startJob();

        // ---------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                RowDisposer.class.getName());
        logger.log(Level.INFO, "Started");

    }

    public void stop() throws Exception {
        internalJob.stopJob();
        internalJob = null;

        disposerPauseTimeMsecs = 0;

        // ---------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                RowDisposer.class.getName());
        logger.log(Level.INFO, "Stopped");
    }
}
