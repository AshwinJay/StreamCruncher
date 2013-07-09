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

import streamcruncher.boot.Registry;
import streamcruncher.innards.InnardsManager;
import streamcruncher.innards.core.stream.InStream;

/*
 * Author: Ashwin Jayaprakash Date: Jul 22, 2006 Time: 12:28:28 PM
 */
/**
 * Each Input Event Stream can have 1 instance, which should be used to notify
 * the Kernel of Events that have been <b>added and committed</b> into the
 * underlying Database. The Kernel does not check if there already is another
 * instance of this Class for the same Input Event Stream. So, the user must
 * take care to <b>create only one instance per Stream</b>. The instances are
 * <b>not Thread-safe</b>. The session can be started and closed and re-started
 * and so on any number of times.
 */
public class InputSession {
    protected String name;

    protected InStream inStream;

    protected InputSession(String name) throws StreamCruncherException {
        this.name = name;

        init();
    }

    private void init() throws StreamCruncherException {
        InnardsManager manager = Registry.getImplFor(InnardsManager.class);
        inStream = manager.getRegisteredInStream(name);

        if (inStream == null) {
            throw new StreamCruncherException("There is no InStream registered under this name.");
        }
    }

    public String getName() {
        return name;
    }

    public void start() throws StreamCruncherException {
        init();
    }

    /**
     * <b>Note:</b> This operation works by assuming that there will be only
     * one Thread invoking this method at a time.
     * 
     * @param event
     *            The Event-Id column must be <b>non-null and unique</b>.
     */
    public void submitEvent(Object[] event) {
        inStream.addEvent(event);
    }

    /**
     * <b>Note:</b> This operation works by assuming that there will be only
     * one Thread invoking this method at a time.
     * 
     * @param events
     *            The Event-Id column must be <b>non-null and unique</b>.
     */
    public void submitEvents(Object[][] events) {
        inStream.addEvents(events);
    }

    public void close() throws StreamCruncherException {
        inStream = null;
    }
}
