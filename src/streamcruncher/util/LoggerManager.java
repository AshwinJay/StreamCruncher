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

import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import streamcruncher.boot.Component;
import streamcruncher.boot.ConfigKeys;

/*
 * Author: Ashwin Jayaprakash Date: Jan 14, 2006 Time: 8:00:51 PM
 */

public class LoggerManager implements Component {
    protected Logger rootLogger;

    /**
     * @return Returns the rootLogger.
     */
    public Logger getRootLogger() {
        return rootLogger;
    }

    public Logger getLogger(String name) {
        return Logger.getLogger(name);
    }

    // ----------------------

    public void start(Object... params) throws Exception {
        Properties properties = (Properties) params[0];
        String filePath = properties.getProperty(ConfigKeys.Logger.CONFIG_FILE_PATH);
        System.setProperty("java.util.logging.config.file", filePath);
        LogManager.getLogManager().readConfiguration();

        rootLogger = Logger.getLogger("");
    }

    public void stop() throws Exception {
        rootLogger = null;
    }
}
