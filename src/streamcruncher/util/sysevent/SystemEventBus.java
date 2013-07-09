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
package streamcruncher.util.sysevent;

import java.util.concurrent.CopyOnWriteArraySet;

import streamcruncher.boot.Component;

/*
 * Author: Ashwin Jayaprakash Date: Oct 28, 2006 Time: 9:52:39 AM
 */

public class SystemEventBus implements Component {
    protected CopyOnWriteArraySet<SystemEventListener> listeners;

    public void submit(SystemEvent event) {
        notifyListeners(event);
    }

    protected void notifyListeners(SystemEvent event) {
        for (SystemEventListener listener : listeners) {
            boolean stop = listener.onEvent(event);
            if (stop) {
                return;
            }
        }
    }

    public void registerEventListener(SystemEventListener listener) {
        listeners.add(listener);
    }

    public void unregisterEventListener(SystemEventListener listener) {
        listeners.remove(listener);
    }

    // -------------

    public void start(Object... params) throws Exception {
        listeners = new CopyOnWriteArraySet<SystemEventListener>();
    }

    public void stop() throws Exception {
        listeners.clear();
        listeners = null;
    }
}
