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
package streamcruncher.api;

import java.util.List;
import java.util.concurrent.TimeUnit;

import streamcruncher.boot.Registry;
import streamcruncher.innards.InnardsManager;
import streamcruncher.innards.core.stream.OutStream;

/*
 * Author: Ashwin Jayaprakash Date: Jul 22, 2006 Time: 12:56:13 PM
 */
/**
 * Each Output Event Stream can have 1 instance, which should be used to
 * retrieve Events that have been <b>added and committed</b> into the
 * underlying Database by the Kernel. The Kernel does not check if there already
 * is another instance of this Class for the same Output Event Stream. So, the
 * user must take care to <b>create only one instance per Stream</b>. The
 * instances are <b>not Thread-safe</b>. The session can be started and closed
 * and re-started and so on any number of times.
 */
public class OutputSession {
    protected final String name;

    protected OutStream outStream;

    public OutputSession(String name) throws StreamCruncherException {
        this.name = name;
    }

    private void init() throws StreamCruncherException {
        InnardsManager manager = Registry.getImplFor(InnardsManager.class);
        outStream = manager.getRegisteredOutStream(name);

        if (outStream == null) {
            throw new StreamCruncherException("There is no OutStream registered under this name.");
        }
    }

    public String getName() {
        return name;
    }

    public void start() throws StreamCruncherException {
        init();
    }

    /**
     * Blocks until at least one Event is available.
     * 
     * @return
     * @throws StreamCruncherException
     */
    public List<Object[]> readEvents() throws StreamCruncherException {
        try {
            return outStream.takeEvents();
        }
        catch (Exception e) {
            throw new StreamCruncherException(e);
        }
    }

    /**
     * @param timeout
     * @param timeUnit
     * @return Empty array if it times out without receiving any new Events.
     * @throws StreamCruncherException
     * @throws InterruptedException
     */
    public List<Object[]> readEvents(long timeout, TimeUnit timeUnit)
            throws StreamCruncherException {
        try {
            return outStream.takeEvents(timeout, timeUnit);
        }
        catch (Exception e) {
            throw new StreamCruncherException(e);
        }
    }

    public void close() throws StreamCruncherException {
        outStream = null;
    }
}
