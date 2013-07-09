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

import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Component;
import streamcruncher.boot.Registry;

/*
 * Author: Ashwin Jayaprakash Date: Mar 4, 2006 Time: 9:49:48 AM
 */
public class TimeKeeper implements Component {
    protected volatile long timeBiasMsecs;

    // ------------

    public void start(Object... params) throws Exception {
        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                TimeKeeper.class.getName());
        logger.log(Level.INFO, "Started");
    }

    public void stop() throws Exception {
        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                TimeKeeper.class.getName());
        logger.log(Level.INFO, "Stopped");
    }

    // ------------

    public long getTimeMsecs() {
        return System.currentTimeMillis() + timeBiasMsecs;
    }

    public long getTimeBiasMsecs() {
        return timeBiasMsecs;
    }

    public void setTimeBiasMsecs(long timeBiasMsecs) {
        this.timeBiasMsecs = timeBiasMsecs;
    }
}
